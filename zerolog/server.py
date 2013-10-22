#!/usr/bin/env python
# encoding: utf-8

"""
- collect messages and publish them to subscribers
- distribute logging config
"""

import sys
import json
import argparse
import logging

import gevent
import gevent.queue
from gevent.pool import Group

import zmq.green as zmq

import zerolog


config = {
    'endpoints': zerolog.default_endpoints,
    'logging': {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simple': {
                'format': '%(asctime)s %(name)s %(levelname)s: %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'NOTSET',
                'formatter': 'simple',
                'stream': 'ext://sys.stdout',
            },
        },
        'root': {
            'level': 'ERROR',
            'handlers': ['console'],
        },
        'loggers': {
            'zerolog': {
                'level': 'ERROR',
            },
            'example': {
                'level': 'INFO',
            },
        },
    },
}


class Dispatcher(gevent.Greenlet):
    def __init__(self, config, context=None, quiet=False):
        super(Dispatcher, self).__init__()
        self.config = config
        self.context = context or zmq.Context()
        self.quiet = quiet
        self.greenlets = Group()
        self.channel = gevent.queue.Queue(0)
        self.subscriptions = []
        self.loggers = {}
        self.log = logging.getLogger('zerolog')
        self._keep_going = True

    def _run(self):
        self.greenlets.add(gevent.spawn(self.__collect))
        self.greenlets.add(gevent.spawn(self.__publish))
        self.greenlets.add(gevent.spawn(self.__control))
        self.greenlets.add(gevent.spawn(self.__subscription_handler))
        self.greenlets.add(gevent.spawn(self.__client_emulator))
        self.greenlets.join()

    def stop(self):
        self._keep_going = False
        self.greenlets.kill()
        self.kill()

    def __collect(self):
        # FIXME: have to decide if this should be a PULL or a SUB socket.
        # using SUB let's the emitter use PUB which doesn't block if HWM is
        # reached
        self.collector = self.context.socket(zmq.SUB)
        self.collector.setsockopt(zmq.SUBSCRIBE, '')
        # then again using PULL / PUSH does block, which may be better to notice
        # that there is a problem
        #self.collector = self.context.socket(zmq.PULL)

        self.collector.bind(self.config['endpoints']['collect'])
        while self._keep_going:
            record_dict = self.collector.recv_json()
            self.channel.put(record_dict)
            gevent.sleep()
        if self.collector:
            self.collector.close()

    def __publish(self):
        self.publisher = self.context.socket(zmq.XPUB)
        self.publisher.hwm = 100000
        # linger: if socket is closed, try sending remaining messages for 1000 milliseconds, then give up
        self.publisher.linger = 1000
        self.publisher.bind(self.config['endpoints']['publish'])
        while self._keep_going:
            record_dict = self.channel.get()
            topic = '{0}{1}.{2}'.format(
                zerolog.stream_prefix,
                record_dict['name'],
                record_dict['levelname']
            )
            if not self.quiet:
                # inject log record into local logger
                logger = zerolog.getLocalLogger(record_dict['name'])
                record = logging.makeLogRecord(record_dict)
                if logger.isEnabledFor(record.levelno):
                    logger.handle(record)
            self.publisher.send_multipart([
                topic.encode('utf-8'),
                json.dumps(record_dict)])
            gevent.sleep()
        if self.publisher:
            self.publisher.close()

    def __control(self):
        self.controller = self.context.socket(zmq.ROUTER)
        self.controller.bind(self.config['endpoints']['control'])
        while self._keep_going:
            address = self.controller.recv()
            empty = self.controller.recv()
            request = self.controller.recv()
            reply = {}
            if request == 'list':
                self.log.debug('received request: {0}'.format(request))
                reply = self.loggers
            else:
                logger_name = self.controller.recv()
                self.log.debug('received request: {0} {1}'.format(request, logger_name))
                if request == 'set':
                    logger_config = self.controller.recv_json()
                    self.loggers[logger_name].update(logger_config)
                    self.configure_logger(logger_name)
                elif request == 'get':
                    try:
                        reply = self.loggers[logger_name]
                    except KeyError as e:
                        self.log.error('could not find config for requested logger: {0}'.format(str(e)))
                        continue
            self.controller.send(address, zmq.SNDMORE)
            self.controller.send('', zmq.SNDMORE)
            self.controller.send_json(reply)
        if self.controller:
            self.controller.close()

    def __subscription_handler(self):
        while self._keep_going:
            try:
                rc = self.publisher.recv()
                subscription = rc[1:]
                status = rc[0] == "\x01"
                if status:
                    self.log.debug('client subscribed to {}'.format(subscription))
                    self.subscriptions.append(subscription)
                    if subscription.startswith(zerolog.config_prefix):
                        self.add_logger(subscription[len(zerolog.config_prefix):])
                else:
                    self.log.debug('client unsubscribed from {}'.format(subscription))
                    self.subscriptions.remove(subscription)
                    if subscription.startswith(zerolog.config_prefix):
                        self.remove_logger(subscription[len(zerolog.config_prefix):])
            except zmq.ZMQError as e:
                print('{0}'.format(e))

    def add_logger(self, logger_name):
        if not logger_name in self.loggers:
            logger_config = self.config.get('logging', {}).get('loggers', {}).get(logger_name, {})
            self.loggers[logger_name] = logger_config
        self.loggers[logger_name]['subscribed'] = True
        self.configure_logger(logger_name)

    def remove_logger(self, logger_name):
        if logger_name in self.loggers:
            self.loggers[logger_name]['subscribed'] = False

    def configure_logger(self, logger_name):
        config = self.loggers[logger_name]
        self.log.debug('configure logger {0} with: {1}'.format(logger_name, config))
        # only configure if config contains more then just the 'subscribed' key
        if len(config) > 1:
            topic = zerolog.config_prefix + logger_name
            self.publisher.send_multipart([
                topic.encode('utf-8'),
                json.dumps(config)
            ])

    @property
    def subscribed_loggers(self):
        for name,config in self.loggers.items():
            if name != 'zerolog' and config['subscribed'] == True:
                yield name

    def __client_emulator(self):
        """Emulate a tool/sysadmin changing log levels.
        """
        levels = 'critical error warning info debug'.split()
        import random
        while self._keep_going:
            loggers = list(self.subscribed_loggers)
            self.log.info('subscribed loggers: {0}'.format(loggers))
            if loggers:
                logger_name = random.choice(list(loggers))
                self.loggers[logger_name].update({
                    'level': random.choice(levels),
                    'propagate': random.choice([0,1]),
                })
                self.configure_logger(logger_name)
            gevent.sleep(5)


class ConfigServer(gevent.Greenlet):
    def __init__(self, config, context=None):
        super(ConfigServer, self).__init__()
        self.config = config
        self.context = context or zmq.Context()
        self.greenlets = Group()
        self._keep_going = True

    def _run(self):
        self.greenlets.add(gevent.spawn(self.__router))
        self.greenlets.add(gevent.spawn(self.__publisher))
        self.greenlets.join()

    def stop(self):
        self._keep_going = False
        self.greenlets.kill()
        self.kill()

    def __router(self):
        self.router = self.context.socket(zmq.ROUTER)
        self.router.bind(self.config['endpoints']['control'])
        while self._keep_going:
            address = self.router.recv()
            empty = self.router.recv()
            request = self.router.recv()
            print('received request: {0}'.format(request))
            self.router.send(address, zmq.SNDMORE)
            self.router.send('', zmq.SNDMORE)
            self.router.send_json(config)
        if self.router:
            self.router.close()


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--log-level', default='warn',
        help='log level. defaults to warn')
    default_format='%(asctime)s %(name)s[%(process)d] %(levelname)s: %(message)s'
    parser.add_argument('--log-format', default=default_format,
        help='log format string. defaults to \'{0}\''.format(default_format.replace('%', '%%')))
    parser.add_argument('-d', '--debug', action='store_true', default=False,
        help='set log level to debug, overrides --log-level')
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
        help='be verbose, set log level to info, overrides --log-level')
    parser.add_argument('-q', '--quiet', action='store_true', default=False,
        help='do not log received messages to stdout')
    #parser.add_argument('config', nargs=1,
    #    help='path to json encoded config file')

    args = parser.parse_args(argv)

    level = getattr(logging, args.log_level.upper())
    logging.basicConfig(level=level, format=args.log_format, datefmt='%Y-%m-%d %H:%M:%S', stream=sys.stdout)

    if args.verbose:
        logging.root.setLevel(logging.INFO)
    # debug overrides verbose
    if args.debug:
        logging.root.setLevel(logging.DEBUG)

    log.debug(args)
    return args


def main(argv=sys.argv[1:]):
    print('this is broken')
    return
    args = parse_args(argv)
    context = zmq.Context()
    #with open(args.config, 'r') as fd:
    #    config = json.load(fd)
    jobs = Group()
    try:
        jobs.start(Dispatcher(config, context=context, quiet=args.quiet))
        jobs.start(ConfigServer(config, context=context))
        jobs.join()
    except KeyboardInterrupt:
        jobs.kill()


def foo():
    job = Dispatcher(config, context=context, quiet=args.quiet)
    try:
        job.start()
        job.join()
    except KeyboardInterrupt:
        job.stop()


if __name__ == '__main__':
    main()

