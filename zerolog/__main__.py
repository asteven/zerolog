# encoding: utf-8
#
# Demo showing zerolog in action
#

import os
import sys
import socket
import random
import logging

import gevent

import zmq.green as zmq


class LogEmitter(gevent.Greenlet):
    """Dumb log emitter that is not configured by the zerolog server.
    """
    def __init__(self, endpoint, context, interval):
        super(LogEmitter, self).__init__()
        self.context = context
        self.interval = interval
        self.socket = self.context.socket(zmq.PUB)
        self.socket.connect(endpoint)
        self._keep_going = True
        self.levels = 'critical error warning info debug'.split()
        config = {
            'version': 1,
            'handlers': {
                'zmq': {
                    'class': 'zerolog.ZerologHandler',
                    'level': 'NOTSET',
                    'endpoint': endpoint,
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


import zerolog
class ZerologEmitter(gevent.Greenlet):
    """Log emitter that is configured by the zerolog server.
    """
    def __init__(self, interval):
        super(ZerologEmitter, self).__init__()
        self.interval = interval
        self._keep_going = True
        config = {
            'version': 1,
             'formatters': {
                'simple': {
                    'format': '%(name)s %(levelname)-8s: %(message)s',
                },
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'level': 'NOTSET',
                    'formatter': 'simple',
                    'stream': 'ext://sys.stdout',
                }
            },
            'root': {
                'level': 'NOTSET',
                'handlers': ['console'],
            },
        }
        import logging.config
        logging.config.dictConfig(config)

    def _run(self):
        #loggers = 'app app.sub app.sub.lib'.split()
        levels = 'critical error warning info debug'.split()
        logger = zerolog.getLogger(__name__)
        index = 0
        while self._keep_going:
            #logger = zerolog.getLogger(random.choice(loggers))
            #logger.propagate = 0
            level = random.choice(levels)
            message = "{0} some {1} message".format(index, level)
            getattr(logger, level)(message)
            index += 1
            gevent.sleep(self.interval)

    def stop(self):
        self._keep_going = False
        self.kill()


def main():
    from zerolog import default_endpoints as endpoints

    context = zmq.Context.instance()

    try:
        name = sys.argv[1]
    except IndexError:
        name = None

    if name.endswith('emit'):
        try:
            interval = float(sys.argv[2])
        except IndexError:
            interval = 1
        if name == 'emit':
            job = LogEmitter(endpoints['collect'].replace('*', 'localhost'), context, interval)
        elif name == 'zeroemit':
            job = ZerologEmitter(interval)
    elif name == 'dispatch':
        from zerolog.server import Dispatcher
        job = Dispatcher(endpoints, context=context)
    elif name == 'tail':
        logging.basicConfig(level=logging.NOTSET, format='%(name)s[%(process)s] %(levelname)-8s: %(message)s', stream=sys.stderr)
        from zerolog.client import LogSubscriber
        job = LogSubscriber(endpoints['publish'].replace('*', 'localhost'), topics=sys.argv[2:], context=context)
    else:
        print("invalid usage : emit|zeroemit|dispatch|tail")
        sys.exit(1)

    try:
        job.start()
        job.join()
    except KeyboardInterrupt:
        job.stop()


if __name__ == '__main__':
    main()
