# encoding: utf-8
#

import sys
import argparse
import logging
import collections

import gevent

import zmq.green as zmq
from zmq.utils.jsonapi import jsonmod as json

import zerolog


class LogSubscriber(gevent.Greenlet):
    """ Subscribe to log dispatcher and emit log records to the local logging system.
    """
    def __init__(self, uri, topics=None, context=None, log_level_name='info'):
        super(LogSubscriber, self).__init__()
        self.uri = uri
        self.topics = topics or ['']
        self.context = context or zmq.Context()
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
            logger_name,level_name = name_and_level.split(':')
            logger = zerolog.getLocalLogger(logger_name)
            if logger.isEnabledFor(logging.getLevelName(level_name)):
                # inject log record into local logger
                record_dict = json.loads(record_json)
                record = logging.makeLogRecord(record_dict)
                logger.handle(record)
        self.socket.close()

    def stop(self):
        self._keep_going = False
        self.kill()


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_true', default=False,
        help='set log level to debug')
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
        help='be verbose, set log level to info')
    parser.add_argument('--uri', required=True,
        help='uri of the zeromq socket on which log messages are published')
    default_format='%(levelname)s: %(name)s: %(message)s'
    parser.add_argument('--log-level', default='info',
        help='log level. defaults to info')
    parser.add_argument('--log-format', default=default_format,
        help='log format string. defaults to {}'.format(default_format.replace('%', '%%')))
    parser.add_argument('topic', default=None,
        nargs='*',
        help='the log topics to subscribe for')

    args = parser.parse_args(argv)
    level = getattr(logging, args.log_level.upper())
    logging.basicConfig(level=level, format=args.log_format, stream=sys.stdout)

    if args.verbose:
        logging.root.setLevel(logging.INFO)
    # debug overrides verbose
    if args.debug:
        logging.root.setLevel(logging.DEBUG)

    return args


def main(argv=sys.argv[1:]):
    args = parse_args(argv)
    context = zmq.Context()
    job = LogSubscriber(zerolog.get_endpoint(args.uri),
        topics=args.topic,
        context=context,
        log_level_name=args.log_level
    )
    try:
        job.start()
        job.join()
    except KeyboardInterrupt:
        job.stop()


if __name__ == '__main__':
    main()
