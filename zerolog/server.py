#!/usr/bin/env python
# encoding: utf-8

"""
- collect messages and publish them to subscribers
- distribute logging config
"""

import sys
import argparse
import logging
import time

import gevent
import gevent.queue
from gevent.pool import Group

import zmq.green as zmq
from zmq.utils.jsonapi import jsonmod as json

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
                'level': 'DEBUG',
            },
            'example': {
                'level': 'INFO',
            },
        },
    },
}

class Dispatcher(gevent.Greenlet):
    def __init__(self, collector, publisher, quiet=False):
        super(Dispatcher, self).__init__()
        self.collector = collector
        self.publisher = publisher
        self.quiet = quiet
        self.greenlets = Group()
        self.channel = gevent.queue.Queue(0)
        self._keep_going = True

    def _run(self):
        self.greenlets.spawn(self.__collect)
        self.greenlets.spawn(self.__publish)
        self.greenlets.join()

    def kill(self, exception=gevent.GreenletExit, **kwargs):
        self._keep_going = False
        self.greenlets.kill()
        super(Dispatcher, self).kill(exception=exception, **kwargs)

    def __collect(self):
        while self._keep_going:
            message = self.collector.recv_multipart()
            self.channel.put(message)
            gevent.sleep()

    def __publish(self):
        while self._keep_going:
            message = self.channel.get()
            if not self.quiet:
                # message is assumed to be a tuple of: (topic, record_json)
                topic,record_json = message
                name_and_level = topic[len(zerolog.stream_prefix):]
                logger_name,level_name = name_and_level.split(':')
                logger = zerolog.getLocalLogger(logger_name)
                if logger.isEnabledFor(logging.getLevelName(level_name)):
                    # inject log record into local logger
                    record_dict = json.loads(record_json)
                    record = logging.makeLogRecord(record_dict)
                    logger.handle(record)
            self.publisher.send_multipart(message)
            gevent.sleep()


class Server(gevent.Greenlet):
    def __init__(self, config, context=None, quiet=False):
        super(Server, self).__init__()
        self.config = config
        self.context = context or zmq.Context()
        self.quiet = quiet

        # dict of the zeromq sockets we use
        self.sockets = {}

        _collect = self.context.socket(zmq.SUB)
        _collect.setsockopt(zmq.SUBSCRIBE, '')
        _collect.bind(self.config['endpoints']['collect'])
        self.sockets['collect'] = _collect

        _publish = self.context.socket(zmq.XPUB)
        _publish.hwm = 100000
        _publish.linger = 1000
        _publish.bind(self.config['endpoints']['publish'])
        self.sockets['publish'] = _publish

        _control = self.context.socket(zmq.ROUTER)
        _control.linger = 0
        _control.bind(self.config['endpoints']['control'])
        self.sockets['control'] = _control

        self.dispatcher = Dispatcher(self.sockets['collect'], self.sockets['publish'], quiet=self.quiet)
        self.controller = Controller(self.sockets['control'], self)

        self.greenlets = Group()
        self.subscriptions = []
        self.loggers = {}
        self.log = logging.getLogger('zerolog')
        self._keep_going = True

    def _run(self):
        self.greenlets.start(self.dispatcher)
        self.greenlets.start(self.controller)
        self.greenlets.add(gevent.spawn(self.__subscription_handler))
        self.greenlets.add(gevent.spawn(self.__client_emulator))
        self.greenlets.join()

    def kill(self, exception=gevent.GreenletExit, **kwargs):
        self._keep_going = False
        self.greenlets.kill()
        for _socket in self.sockets.values():
            _socket.close()
        super(Server, self).kill(exception=exception, **kwargs)

    def __subscription_handler(self):
        while self._keep_going:
            try:
                rc = self.sockets['publish'].recv()
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


class Controller(gevent.Greenlet):
    def __init__(self, control_socket, handler):
        super(Controller, self).__init__()
        self.socket = control_socket
        self.handler = handler
        self.log = logging.getLogger('zerolog')
        self._keep_going = True

    def send_error(self, mid, cid, reason='unknown', tb=None):
        response = {
            'status': 'error',
            'reason': reason,
            'tb': tb,
            'time': time.time(),
        }
        self.send_message(mid, cid, response)

    def send_response(self, mid, cid, message):
        response = {
            'status': 'ok',
            'time': time.time(),
        }
        response.update(message)
        self.send_message(mid, cid, response)

    def send_message(self, mid, cid, message):
        self.log.debug('send_message: mid: {0}, cid: {1}, message: {2}'.format(mid, cid, message))
        message['id'] = mid
        try:
            self.socket.send(cid, zmq.SNDMORE)
            self.socket.send_json(message)
        except (ValueError, zmq.ZMQError) as e:
            self.log.error('Failed to send message to {0}: {1}'.format(cid, message))

    def _run(self):
        while self._keep_going:
            client_id = self.socket.recv()
            try:
                request = self.socket.recv_json()
                message_id = request['id']
                # TODO: see netboot/tftp/server.py TftpListener for how to dispatch to handler
                response = request
                self.send_response(message_id, client_id, response)
            except (ValueError, zmq.ZMQError) as e:
                self.send_error(mid, cid, str(e))
        if self.socket:
            self.socket.close()

    def kill(self, exception=gevent.GreenletExit, **kwargs):
        self._keep_going = False
        super(Controller, self).kill(exception=exception, **kwargs)

    def foo(self):
        address = self.controller.recv()
        empty = self.controller.recv()
        request = self.controller.recv()
        reply = {}
        if request == 'endpoints':
            reply = self.config['endpoints']
        elif request == 'list':
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
        self.controller.send(client_id, zmq.SNDMORE)
        self.controller.send('', zmq.SNDMORE)
        self.controller.send_json(reply)



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
        jobs.start(Server(config, context=context, quiet=args.quiet))
        jobs.join()
    except KeyboardInterrupt:
        jobs.kill()


def foo():
    job = Server(config, context=context, quiet=args.quiet)
    try:
        job.start()
        job.join()
    except KeyboardInterrupt:
        job.kill()


if __name__ == '__main__':
    main()

