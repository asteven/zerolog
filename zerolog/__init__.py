# encoding: utf-8

import os
import sys
import socket
import logging

import gevent
import gevent.queue

import zmq.green as zmq

from . import compat


# Make sure a NullHandler is available
# This was added in Python 2.7/3.2
try:
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass


# prefixes to distinguish between log and config subscriptions
stream_prefix = 'stream.'
config_prefix = 'config.'


# default endpoints
default_endpoints = {
    'collect': 'tcp://127.0.0.42:42231',
    'publish': 'tcp://127.0.0.42:42232',
}


def get_endpoint(endpoint):
    """
    Create directories for ipc endpoints as needed.
    """
    if endpoint.startswith('ipc://'):
        dirname = os.path.dirname(endpoint[6:])
        compat.makedirs(dirname, exist_ok=True)
    return endpoint


def configure(endpoints=None, context=None):
    """Configure python logging to be zerolog aware.

    This replaces the logging.Logger.manager by one that knows how to get
    logger configs from a zerolog server.
    """
    logging.Logger.manager = ZerologManager(
        logging.Logger.manager,
        endpoints or default_endpoints,
        context=context
    )


class ZerologManager(logging.Manager):
    def __init__(self, manager, endpoints, context=None):
        super(ZerologManager, self).__init__(manager.root)
        self.inherit(manager)
        self.endpoints = endpoints
        self.context = context or zmq.Context.instance()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect(self.endpoints['publish'])
        self.zerolog_handler = ZerologHandler(self.endpoints['collect'], context=self.context)
        #self.zerolog_handler = GreenZerologHandler(self.endpoints['collect'], context=self.context)
        self._keep_going = True
        gevent.spawn(self.__configure)

    def __configure(self, *args):
        while self._keep_going:
            topic, level = self.socket.recv_multipart()
            logger_name = topic.lstrip(config_prefix)
            logger = self.getLogger(logger_name)
            logger.setLevel(level.upper())

    def subscribe(self, name):
        self.socket.setsockopt(zmq.SUBSCRIBE, config_prefix + name)

    def getLogger(self, name):
        subscribe = (not name in self.loggerDict \
            or isinstance(self.loggerDict[name], logging.PlaceHolder)
        )
        logger = super(ZerologManager, self).getLogger(name)
        if not self.zerolog_handler in logger.handlers:
            logger.addHandler(self.zerolog_handler)
        if subscribe:
            self.subscribe(name)
        return logger

    def inherit(self, other_manager):
        for attr,value in other_manager.__dict__.items():
            if attr != 'loggerDict':
                setattr(self, attr, value)
        for name,logger in other_manager.loggerDict.items():
            self.loggerDict[name] = logger
            if not isinstance(logger, logging.PlaceHolder):
                self.subscribe(name)


def getLogger(name=None):
    """Get a logger with a ZerologHandler attached.

    Configures zerolog to use default endpoints if not already done explicitly.
    """
    if not isinstance(logging.Logger.manager, ZerologManager):
        configure()
    return logging.getLogger(name)


def getLocalLogger(name=None):
    """Get a logger that has at least a NullHandler attached.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.addHandler(NullHandler())
    return logger


class ZerologHandler(logging.Handler):
    """ A logging handler for sending notifications to a ømq PUSH socket.
    """
    def __init__(self, endpoint, context=None):
        self.endpoint = endpoint
        self.context = context or zmq.Context.instance()
        self.hostname = socket.gethostname()
        # FIXME: should this be a PUSH or a PUB socket?
        #   PUSH blocks whenn HWM is reached
        #   PUB drops messages when HWM is reached
        # what is better?
        #self.socket = self.context.socket(zmq.PUSH)
        self.socket = self.context.socket(zmq.PUB)
        self.socket.hwm = 10000
        # linger: if socket is closed, try sending remaining messages for 1000 milliseconds, then give up
        self.socket.linger = 1000
        self.socket.connect(self.endpoint)
        super(ZerologHandler, self).__init__()

    def close(self):
        self.socket.close()

    def emit(self, record):
        """
        Emit a record.

        Writes the LogRecord to the queue, preparing it for pickling first.
        """
        try:
            # The format operation gets traceback text into record.exc_text
            # (if there's exception data), and also puts the message into
            # record.message. We can then use this to replace the original
            # msg + args, as these might be unpickleable. We also zap the
            # exc_info attribute, as it's no longer needed and, if not None,
            # will typically not be pickleable.
            self.format(record)
            record.msg = record.message
            record.args = None
            record.exc_info = None
            self.__send(record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def __send(self, record):
        message = record.__dict__
        # add hostname to record
        message['hostname'] = self.hostname
        try:
            self.socket.send_json(message)
        except:
            self.handleError(record)


class GreenZerologHandler(logging.Handler):
    """ A logging handler for sending notifications to a ømq PUSH socket.
    """
    def __init__(self, endpoint, context=None):
        self.endpoint = endpoint
        self.context = context or zmq.Context.instance()
        self.hostname = socket.gethostname()
        # FIXME: should this be a PUSH or a PUB socket?
        #   PUSH blocks whenn HWM is reached
        #   PUB drops messages when HWM is reached
        # what is better?
        #self.socket = self.context.socket(zmq.PUSH)
        self.socket = self.context.socket(zmq.PUB)
        self.socket.hwm = 10000
        # linger: if socket is closed, try sending remaining messages for 1000 milliseconds, then give up
        self.socket.linger = 1000
        self.socket.connect(self.endpoint)
        # channel queue, put always blocks until delivered
        self.channel = gevent.queue.Queue(0)
        self._job = gevent.spawn(self.__send)
        super(GreenZerologHandler, self).__init__()

    def close(self):
        self._job.kill(timeout=2)
        self.socket.close()

    def emit(self, record):
        """
        Emit a record.

        Writes the LogRecord to the queue, preparing it for pickling first.
        """
        try:
            # The format operation gets traceback text into record.exc_text
            # (if there's exception data), and also puts the message into
            # record.message. We can then use this to replace the original
            # msg + args, as these might be unpickleable. We also zap the
            # exc_info attribute, as it's no longer needed and, if not None,
            # will typically not be pickleable.
            self.format(record)
            record.msg = record.message
            record.args = None
            record.exc_info = None
            self.channel.put(record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def __send(self):
        while True:
            record = self.channel.get()
            message = record.__dict__
            # add hostname to record
            message['hostname'] = self.hostname
            try:
                self.socket.send_json(message)
            except:
                self.handleError(record)
