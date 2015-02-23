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
from gevent.pool import Group

import zmq.green as zmq


class LogEmitter(gevent.Greenlet):
    """Dumb log emitter that is not configured by the zerolog server.
    """
    def __init__(self, endpoint, interval, context=None):
        super(LogEmitter, self).__init__()
        self.context = context or zmq.Context.instance()
        self.interval = interval
        self.socket = self.context.socket(zmq.PUB)
        self.socket.connect(endpoint)
        self._keep_going = True
        self.levels = 'critical error warning info debug'.split()
        config = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'simple': {
                    'format': '[%(process)s] %(name)-15s %(levelname)-8s: %(message)s',
                },
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'level': 'NOTSET',
                    'formatter': 'simple',
                    'stream': 'ext://sys.stdout',
                },
                'zmq': {
                    'class': 'zerolog.ZerologHandler',
                    'level': 'NOTSET',
                    'endpoint': endpoint,
                    'context': self.context,
                },
            },
            'root': {
                'level': 'NOTSET',
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

    def kill(self, exception=gevent.GreenletExit, **kwargs):
        self._keep_going = False
        super(LogEmitter, self).kill(exception=exception, **kwargs)


import zerolog
class ZerologEmitter(gevent.Greenlet):
    """Log emitter that is configured by the zerolog server.
    """
    def __init__(self, interval):
        super(ZerologEmitter, self).__init__()
        self.interval = interval
        self._keep_going = True

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

    def kill(self, exception=gevent.GreenletExit, **kwargs):
        self._keep_going = False
        super(ZerologEmitter, self).kill(exception=exception, **kwargs)


class MultiZerologEmitter(gevent.Greenlet):
    """Emitter using multiple loggers which are configured by the zerolog server.
    """
    def __init__(self, interval):
        super(MultiZerologEmitter, self).__init__()
        self.interval = interval
        self.greenlets = Group()
        #self.loggers = 'foo foo.lib foo.web foo.web.request foo.web.db'.split()
        self.loggers = 'foo foo.lib foo.lib.bar'.split()
        self.levels = 'critical error warning info debug'.split()
        self._keep_going = True

    def _run(self):
        self.greenlets.add(gevent.spawn(self.__random_logger))
        #for logger_name in self.loggers:
        #    self.greenlets.add(gevent.spawn(self.__logger, logger_name))
        self.greenlets.join()

    def __logger(self, logger_name):
        #loggers = 'app app.sub app.sub.lib'.split()
        logger = zerolog.getLogger(logger_name)
        index = 0
        while self._keep_going:
            level = random.choice(self.levels)
            message = "{0} {1} {2}".format(index, logger_name, level)
            getattr(logger, level)(message)
            index += 1
            gevent.sleep(self.interval)

    def __random_logger(self):
        index = 0
        while self._keep_going:
            logger = zerolog.getLogger(random.choice(self.loggers))
            level = random.choice(self.levels)
            message = "{0} {1} {2}".format(index, logger.name, level)
            getattr(logger, level)(message)
            index += 1
            gevent.sleep(self.interval)

    def kill(self, exception=gevent.GreenletExit, **kwargs):
        self._keep_going = False
        self.greenlets.kill()
        super(MultiZerologEmitter, self).kill(exception=exception, **kwargs)


class DebugZerologEmitter(gevent.Greenlet):
    def __init__(self, interval):
        super(DebugZerologEmitter, self).__init__()
        self.interval = interval
        self._keep_going = True

    def _run(self):
        #loggers = 'app app.sub app.sub.lib'.split()
        levels = 'critical error warning info debug'.split()
        logger = zerolog.getLogger('zerolog')
        index = 0
        while self._keep_going:
            #logger = zerolog.getLogger(random.choice(loggers))
            #logger.propagate = 0
            level = random.choice(levels)
            message = "{0} some {1} message".format(index, level)
            getattr(logger, level)(message)
            index += 1
            gevent.sleep(self.interval)

    def kill(self, exception=gevent.GreenletExit, **kwargs):
        self._keep_going = False
        super(DebugZerologEmitter, self).kill(exception=exception, **kwargs)


def main():
    logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simple': {
                'format': '[%(process)s] %(name)-15s %(levelname)-8s: %(message)s',
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
    logging.config.dictConfig(logging_config)


    from zerolog import default_endpoints as endpoints

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
            job = LogEmitter(endpoints['collect'].replace('*', 'localhost'), interval)
        elif name == 'zeroemit':
            job = ZerologEmitter(interval)
        elif name == 'multiemit':
            job = MultiZerologEmitter(interval)
        elif name == 'debugemit':
            job = DebugZerologEmitter(interval)
    elif name == 'dispatch':
        from zerolog.server import Server
        from zerolog.server import default_config as config
        job = Server(config, quiet=True)
    elif name == 'tail':
        from zerolog.client import LogSubscriber
        job = LogSubscriber(endpoints['publish'].replace('*', 'localhost'), topics=sys.argv[2:])
    else:
        print("invalid usage : dispatch|tail|emit|zeroemit|multiemit|debugemit")
        sys.exit(1)

    try:
        job.start()
        job.join()
    except KeyboardInterrupt:
        job.kill()


if __name__ == '__main__':
    main()
