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
import json

import gevent
import gevent.queue
from gevent.pool import Group

import zmq.green as zmq

import zerolog
from zerolog import errors


default_config = {
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
        self.context = context or zmq.Context.instance()
        self.quiet = quiet

        # dict of the zeromq sockets we use
        self.sockets = {}

        _collect = self.context.socket(zmq.SUB)
        _collect.setsockopt(zmq.SUBSCRIBE, '')
        _collect.bind(zerolog.get_endpoint(self.config['endpoints']['collect']))
        self.sockets['collect'] = _collect

        _publish = self.context.socket(zmq.XPUB)
        _publish.hwm = 100000
        _publish.linger = 1000
        _publish.setsockopt(zmq.XPUB_VERBOSE, 1)
        _publish.bind(zerolog.get_endpoint(self.config['endpoints']['publish']))
        self.sockets['publish'] = _publish

        _control = self.context.socket(zmq.ROUTER)
        _control.linger = 0
        _control.bind(zerolog.get_endpoint(self.config['endpoints']['control']))
        self.sockets['control'] = _control

        self.manager = ConfigManager(self.sockets['publish'], self.config)
        self.controller = Controller(self.sockets['control'], self.manager)
        self.dispatcher = Dispatcher(self.sockets['collect'], self.sockets['publish'], quiet=self.quiet)

        self.greenlets = Group()
        self.log = logging.getLogger('zerolog')
        self._keep_going = True

    def _run(self):
        self.greenlets.start(self.manager)
        self.greenlets.start(self.controller)
        self.greenlets.start(self.dispatcher)
        #self.greenlets.add(gevent.spawn(self.__client_emulator))
        self.greenlets.join()

    def kill(self, exception=gevent.GreenletExit, **kwargs):
        self._keep_going = False
        self.greenlets.kill()
        for _socket in self.sockets.values():
            _socket.close()
        super(Server, self).kill(exception=exception, **kwargs)

    def __client_emulator(self):
        """Emulate a tool/sysadmin changing log levels.
        """
        levels = 'critical error warning info debug'.split()
        import random
        while self._keep_going:
            loggers = list(self.manager.subscribed_loggers)
            self.log.info('subscribed loggers: {0}'.format(loggers))
            if loggers:
                logger_name = random.choice(list(loggers))
                self.manager.update(logger_name, {
                    'level': random.choice(levels),
                    'propagate': random.choice([0,1]),
                })
                self.manager.configure(logger_name)
            gevent.sleep(5)


class ConfigManager(gevent.Greenlet):
    def __init__(self, publish_socket, config):
        super(ConfigManager, self).__init__()
        self.socket = publish_socket
        self.config = config
        self.subscriptions = []
        self.loggers = {}
        self.log = logging.getLogger('zerolog')
        self._keep_going = True

    def _run(self):
        while self._keep_going:
            try:
                rc = self.socket.recv()
                subscription = rc[1:]
                status = rc[0] == "\x01"
                if status:
                    self.log.debug('client subscribed to {}'.format(subscription))
                    self.subscriptions.append(subscription)
                    if subscription.startswith(zerolog.config_prefix):
                        self.add(subscription[len(zerolog.config_prefix):])
                else:
                    self.log.debug('client unsubscribed from {}'.format(subscription))
                    self.subscriptions.remove(subscription)
            except zmq.ZMQError as e:
                self.log.error('{0}'.format(e))

    def kill(self, exception=gevent.GreenletExit, **kwargs):
        self._keep_going = False
        super(ConfigManager, self).kill(exception=exception, **kwargs)

    def configure_parent_with_config(self, logger_name):
        """Find a parent of the the given logger which has a config.
        """
        name = logger_name
        while name:
            name = name.rpartition('.')[0]
            config = self.get(name)
            if config:
                self.configure(name)
                break

    def add(self, logger_name):
        """Add a logger to the manager and configure it or it's parent.
        """
        config = self.get(logger_name)
        if config:
            # this logger has it's own config, publish it
            self.configure(logger_name)
        else:
            # this logger has no own config
            # check if we have a config for one of it's parents
            self.configure_parent_with_config(logger_name)

    def update(self, logger_name, logger_config):
        """Update the given loggers configuration.
        """
        if logger_name in self.loggers:
            self.loggers[logger_name].update(logger_config)
            self.configure(logger_name)

    def get(self, logger_name):
        """Get the given loggers configuration.
        """
        if not logger_name in self.loggers:
            logger_config = self.config.get('logging', {}).get('loggers', {}).get(logger_name, {})
            self.loggers[logger_name] = logger_config
        return self.loggers[logger_name]

    def set(self, logger_name, logger_config):
        """Set the given loggers configuration.
        """
        self.loggers[logger_name] = logger_config
        self.configure(logger_name)

    def configure(self, logger_name):
        """Publish the current configuration to the given named logger.
        """
        config = self.get(logger_name)
        if config:
            self.log.debug('configure logger {0} with: {1}'.format(logger_name, config))
            topic = zerolog.config_prefix + logger_name
            self.socket.send_multipart([
                topic.encode('utf-8'),
                json.dumps(config)
            ])

    @property
    def subscribed_loggers(self):
        for name,config in self.loggers.items():
            if name != 'zerolog' and config['subscribed'] == True:
                yield name


class Controller(gevent.Greenlet):
    def __init__(self, control_socket, manager):
        super(Controller, self).__init__()
        self.socket = control_socket
        self.manager = manager
        self.log = logging.getLogger('zerolog')
        self._keep_going = True

    def _run(self):
        while self._keep_going:
            client_id = self.socket.recv()
            try:
                message = self.socket.recv()
                message = message.strip()
                self.log.debug('<< {0}: {1}'.format(client_id, message))
                if message:
                    self.dispatch(client_id, message)
            except (ValueError, zmq.ZMQError) as e:
                self.send_error(client_id, None, str(e))
        if self.socket:
            self.socket.close()

    def kill(self, exception=gevent.GreenletExit, **kwargs):
        self._keep_going = False
        super(Controller, self).kill(exception=exception, **kwargs)

    def dispatch(self, client_id, message):
        try:
            json_msg = json.loads(message)
        except ValueError:
            return self.send_error(client_id, None, message, 'json invalid',
                                   errno=errors.INVALID_JSON)
        message_id = json_msg['id']
        command = json_msg['command']
        arguments = json_msg.get('args', {})

        try:
            command_handler = getattr(self, 'handle_{}'.format(command))
        except AttributeError as e:
            error_message = 'unknown command: {0}'.format(command)
            self.log.debug(error_message)
            return self.send_error(client_id, message_id, error_message)
        try:
            # Do _not_ spawn another greenlet here.
            response = command_handler(arguments)
            self.send_response(client_id, message_id, response)
        except Exception as e:
            self.log.error('Uncaught Exception: {0}'.format(e), exc_info=True)
            self.send_error(client_id, message_id, 'Internal Server Error', errno=errors.OS_ERROR)
            raise

    def send_error(self, client_id, message_id, reason, traceback=None, errno=errors.NOT_SPECIFIED):
        message = {
            'status': 'error',
            'reason': reason,
            'traceback': traceback,
            'errno': errno,
        }
        self.send_message(client_id, message_id, message)

    def send_response(self, client_id, message_id, response):
        message = {
            'status': 'ok',
            'response': response,
        }
        self.send_message(client_id, message_id, message)

    def send_message(self, client_id, message_id, message):
        message['id'] = message_id
        message['time'] = time.time()
        self.log.debug('>> {0}: {1}'.format(client_id, message))
        try:
            self.socket.send(client_id, zmq.SNDMORE)
            self.socket.send_json(message)
        except (ValueError, zmq.ZMQError) as e:
            self.log.error('Failed to send message to {0}: {1}'.format(client_id, message))

    def handle_endpoints(self, args):
        return self.manager.config['endpoints']

    def handle_list(self, args):
        return self.manager.loggers

    def handle_get(self, args):
        name = args.get('name', None)
        return self.manager.get(name)

    def handle_set(self, args):
        name = args.get('name', None)
        config = args.get('config', None)
        return self.manager.set(name, config)

    def handle_update(self, client_id, message_id, request):
        name = args.get('name', None)
        config = args.get('config', None)
        return self.manager.update(name, config)


def parse_args(argv):
    parser = argparse.ArgumentParser()
    default_format='%(asctime)s %(name)s[%(process)d] %(levelname)s: %(message)s'
    parser.add_argument('--log-format', default=default_format,
        help='log format string. defaults to \'{0}\''.format(default_format.replace('%', '%%')))
    loglevel_parser = parser.add_mutually_exclusive_group(required=False)
    loglevel_parser.add_argument('--log-level', dest='log_level', default='warn',
        help='log level. defaults to warn')
    loglevel_parser.add_argument('-d', '--debug', action='store_const', const='debug', dest='log_level',
        help='set log level to debug')
    loglevel_parser.add_argument('-v', '--verbose', action='store_const', const='info', dest='log_level',
        help='be verbose, set log level to info')
    parser.add_argument('-q', '--quiet', action='store_true', default=False,
        help='do not log received messages to stdout')
    parser.add_argument('-c', '--config', help='path to json config file')

    args = parser.parse_args(argv)

    level = logging.getLevelName(args.log_level.upper())
    logging.basicConfig(level=level, format=args.log_format, datefmt='%Y-%m-%d %H:%M:%S', stream=sys.stdout)

    log = logging.getLogger('zerolog')
    log.debug(args)
    return args


def main(argv=sys.argv[1:]):
    args = parse_args(argv)

    if args.config:
        with open(args.config, 'r') as fd:
            config = json.load(fd)
    else:
        from zerolog.server import default_config as config

    job = Server(config, quiet=args.quiet)
    try:
        job.start()
        job.join()
    except KeyboardInterrupt:
        job.kill()


if __name__ == '__main__':
    main()

