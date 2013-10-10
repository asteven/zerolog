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
   'endpoints': {
      'control': 'tcp://127.0.0.42:6660',
      'collect': 'tcp://127.0.0.42:6661',
      'publish': 'tcp://127.0.0.42:6662',
   },
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
   }
}


class Dispatcher(gevent.Greenlet):
    def __init__(self, endpoints, context=None, quiet=False):
        super(Dispatcher, self).__init__()
        self.endpoints = endpoints
        self.context = context or zmq.Context()
        self.greenlets = Group()
        self.channel = gevent.queue.Queue(0)
        self._keep_going = True
        self.quiet = quiet
        self.subscriptions = []

    def _run(self):
        self.greenlets.add(gevent.spawn(self.__collect))
        self.greenlets.add(gevent.spawn(self.__publish))
        self.greenlets.add(gevent.spawn(self.__configure))
        self.greenlets.add(gevent.spawn(self.__client_emulator))
        self.greenlets.join()

    def stop(self):
        self._keep_going = False
        self.greenlets.kill()
        self.kill()

    def __collect(self):
        # FIXME: should this be a PULL or a SUB socket?
        # using SUB let's the emitter use PUB which doesn't block if HWM is
        # reached
        self.collector = self.context.socket(zmq.SUB)
        self.collector.setsockopt(zmq.SUBSCRIBE, '')
        # then again using PULL / PUSH does block, which may be better to notice
        # that there is a problem
        #self.collector = self.context.socket(zmq.PULL)

        self.collector.bind(self.endpoints['collect'])
        while self._keep_going:
            record_dict = self.collector.recv_json()
            self.channel.put(record_dict)
            # FIXME: is this sleep needed? Why?
            gevent.sleep()
        if self.collector:
            self.collector.close()

    def __publish(self):
        self.publisher = self.context.socket(zmq.XPUB)
        self.publisher.hwm = 100000
        # linger: if socket is closed, try sending remaining messages for 1000 milliseconds, then give up
        self.publisher.linger = 1000
        self.publisher.bind(self.endpoints['publish'])
        while self._keep_going:
            record_dict = self.channel.get()
            topic = '.'.join((record_dict['name'], record_dict['levelname']))
            topic = zerolog.stream_prefix + topic
            topic = topic.encode('utf-8')
            if not self.quiet:
                # inject log record into local logger
                record = logging.makeLogRecord(record_dict)
                logger_name = topic.split('.')[0]
                logger = zerolog.getLocalLogger(logger_name)
                if logger.isEnabledFor(record.levelno):
                    logger.handle(record)
            self.publisher.send_multipart([
                topic,
                json.dumps(record_dict)])
            # FIXME: is this sleep needed? Why?
            gevent.sleep()
        if self.publisher:
            self.publisher.close()

    def __configure(self):
        while self._keep_going:
            try:
                rc = self.publisher.recv()
                subscription = rc[1:]
                status = rc[0] == "\x01"
                if status:
                    print('client subscribed to {}'.format(subscription))
                    self.subscriptions.append(subscription)
                else:
                    print('client unsubscribed from {}'.format(subscription))
                    self.subscriptions.remove(subscription)
            except zmq.ZMQError as e:
                print('{0}'.format(e))

    @property
    def loggers(self):
        for subscription in self.subscriptions:
            if subscription.startswith(zerolog.config_prefix):
                yield subscription.lstrip(zerolog.config_prefix)

    def __client_emulator(self):
        """Emulate a tool/sysadmin changing log levels.
        """
        levels = 'critical error warning info debug'.split()
        import random
        while self._keep_going:
            level = random.choice(levels)
            for logger_name in self.loggers:
                print('sending {0} to {1}'.format(level, logger_name))
                topic = zerolog.config_prefix + logger_name
                self.publisher.send_multipart([
                    topic,
                    level
                ])
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

