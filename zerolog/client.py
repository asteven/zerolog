# encoding: utf-8
#

import sys
import argparse
import logging
import collections
import uuid
import time
import json

import gevent

import zmq.green as zmq

import zerolog


class CallError(Exception):
    pass


class ZerologClient(object):
    def __init__(self, context=None,
            endpoint=zerolog.default_endpoints['control'],
            timeout=5.0):
        self.context = context or zmq.Context.instance()
        self.endpoint = endpoint
        self._id = uuid.uuid4().hex
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.setsockopt_string(zmq.IDENTITY, self._id)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.connect(self.endpoint)
        self._timeout = timeout
        self.timeout = timeout * 1000

    def stop(self):
        self.socket.close()

    def call(self, command, args=None):
        message = {
            'command': command,
            'args': args or {},
        }
        return self.send_message(message)

    def send_message(self, message):
        message_id = uuid.uuid4().hex
        message['id'] = message_id
        message['time'] = time.time()

        try:
            self.socket.send_json(message)
        except zmq.ZMQError as e:
            raise CallError(str(e))

        while True:
            try:
                message = self.socket.recv()
                response = json.loads(message.decode())
                if response['id'] != message_id:
                    # we got the wrong message
                    continue
                return response
            except ValueError as e:
                raise CallError(str(e))


class LogSubscriber(gevent.Greenlet):
    """ Subscribe to log dispatcher and emit log records to the local logging system.
    """
    def __init__(self, uri, topics=None, context=None, log_level_name='info'):
        super(LogSubscriber, self).__init__()
        self.uri = uri
        self.topics = topics or [b'']
        self.context = context or zmq.Context.instance()
        self.log_level_name = log_level_name
        self.log_level = logging.getLevelName(self.log_level_name.upper())
        self._keep_going = True

    def _run(self):
        self.socket = self.context.socket(zmq.SUB)
        if not isinstance(self.topics, collections.Iterable):
            self.topics = [self.topics]
        for topic in self.topics:
            self.socket.setsockopt(zmq.SUBSCRIBE, zerolog.stream_prefix + topic)
        self.socket.connect(self.uri)
        while self._keep_going:
            topic,record_json = self.socket.recv_multipart()
            name_and_level = topic[len(zerolog.stream_prefix):]
            logger_name,level_name = name_and_level.split(b':')
            logger = zerolog.getLocalLogger(logger_name)
            if logger.isEnabledFor(logging.getLevelName(level_name)):
                # inject log record into local logger
                record_dict = json.loads(record_json)
                record = logging.makeLogRecord(record_dict)
                logger.handle(record)
        self.socket.close()

    def kill(self, exception=gevent.GreenletExit, **kwargs):
        self._keep_going = False
        super(LogSubscriber, self).kill(exception=exception, **kwargs)


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--endpoint', required=False,
        help='uri of the zerolog control socket')
    default_log_format = '[%(process)s] %(name)s: %(levelname)s: %(message)s'
    parser.add_argument('--log-format', default=default_log_format,
        help='log format string. defaults to {}'.format(default_log_format.replace('%', '%%')))
    loglevel_parser = parser.add_mutually_exclusive_group(required=False)
    loglevel_parser.add_argument('--log-level', dest='log_level', default='info',
        help='log level. defaults to info')
    loglevel_parser.add_argument('-d', '--debug', action='store_const', const='debug', dest='log_level',
        help='set log level to debug')
    loglevel_parser.add_argument('-v', '--verbose', action='store_const', const='info', dest='log_level',
        help='be verbose, set log level to info')
    parser.add_argument('topic', default=None, type=bytes,
        nargs='*',
        help='one or more logger names to subscribe for')

    args = parser.parse_args(argv)
    level = logging.getLevelName(args.log_level.upper())
    logging.basicConfig(level=level, format=args.log_format, stream=sys.stdout)

    return args


def main(argv=sys.argv[1:]):
    args = parse_args(argv)
    if args.endpoint:
        endpoints = zerolog.get_endpoints_from_controller(args.endpoint)
    else:
        endpoints = zerolog.default_endpoints

    job = LogSubscriber(
        zerolog.get_endpoint(endpoints['publish']),
        topics=args.topic,
        log_level_name=args.log_level
    )
    try:
        job.start()
        job.join()
    except KeyboardInterrupt:
        job.kill()


if __name__ == '__main__':
    main()
