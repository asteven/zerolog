# encoding: utf-8
#
# Demo showing zerolog in action
#

import os
import sys
import socket
import logging

import gevent

import zmq.green as zmq


class LogEmitter(gevent.Greenlet):
    def __init__(self, uri, context, interval):
        super(LogEmitter, self).__init__()
        self.context = context
        self.interval = interval
        self.socket = self.context.socket(zmq.PUSH)
        self.socket.connect(uri)
        self._keep_going = True
        self.levels = 'critical error warning info debug'.split()
        config = {
            'version': 1,
            'handlers': {
                'zmq': {
                    'class': 'zerolog.ZmqHandler',
                    'level': 'NOTSET',
                    'uri': uri,
                    'context': self.context,
                },
                'console': {
                    'class': 'logging.StreamHandler',
                    'level': 'DEBUG',
                    'stream': 'ext://sys.stdout',
                }
            },
            'root': {
                'level': 'NOTSET',
                #'handlers': ['console'],
                'handlers': ['console', 'zmq'],
            },
        }
        import logging.config
        logging.config.dictConfig(config)

    def _run(self):
        import random
        logger = logging.getLogger(__name__)
        index = 0
        while self._keep_going:
            level = random.choice(self.levels)
            message = "{0} some {1} message".format(index, level)
            getattr(logger, level)(message)
            index += 1
            gevent.sleep(self.interval)

    def stop(self):
        self._keep_going = False
        self.kill()


def main():
    from zerolog.server import config

    context = zmq.Context()

    try:
        name = sys.argv[1]
    except IndexError:
        name = None

    if name == 'emit':
        try:
            interval = float(sys.argv[2])
        except IndexError:
            interval = 1
        job = LogEmitter(config['endpoints']['collect'].replace('*', 'localhost'), context, interval)
    elif name == 'dispatch':
        from zerolog.server import Dispatcher
        job = Dispatcher(config, context=context)
    elif name == 'config':
        from zerolog.server import ConfigServer
        job = ConfigServer(config, context=context)
    elif name == 'tail':
        logging.basicConfig(level=logging.NOTSET, format='%(name)s[%(process)s] %(levelname)s: %(message)s', stream=sys.stderr)
        from zerolog.client import LogSubscriber
        job = LogSubscriber(config['endpoints']['publish'].replace('*', 'localhost'), topics=sys.argv[2:], context=context)
    else:
        print("invalid usage : emit|dispatch|config|tail")
        sys.exit(1)

    try:
        job.start()
        job.join()
    except KeyboardInterrupt:
        job.stop()


if __name__ == '__main__':
    main()
