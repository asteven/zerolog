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


def get_endpoint(endpoint):
    """
    Create directories for ipc endpoints as needed.
    """
    if endpoint.startswith('ipc://'):
        dirname = os.path.dirname(endpoint[6:])
        compat.makedirs(dirname, exist_ok=True)
    return endpoint


_control_uri = None
_context = None
_handler = None
def configure(uri, context=None):
    _control_uri = uri
    _context = context or zmq.Context()
    _handler = ZmqHandler(_control_uri, context=_context)


class NotConfiguredError(Exception):
    """Raised if zerolog is used before it has been configured.
    """


class ZeroLogger(logging.Logger):
    def __init__(self, name, level=NOTSET):
        super(ZeroLogger, self).__init__(name, level)



def getLogger(name=None):
    """Get a logger with a ZmqHandler attached.
    """
    if _control_uri is None:
        raise NotConfiguredError('Zerolog is not configured. Use zerolog.configure(uri).')
    logger = logging.getLogger(name)
    if not _handler in logger.handlers:
        logger.addHandler(_handler)
    return logger


def getLocalLogger(name=None):
    """Get a logger that has at least a NullHandler attached.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.addHandler(NullHandler())
    return logger


class ZmqHandler(logging.Handler):
    """ A logging handler for sending notifications to a ømq PUSH socket.
    """
    def __init__(self, uri, context=None):
        self.uri = uri
        self.context = context or zmq.Context()
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
        self.socket.connect(self.uri)
        # channel queue, put always blocks until delivered
        self.channel = gevent.queue.Queue(0)
        self._job = gevent.spawn(self.__send)
        super(ZmqHandler, self).__init__()

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
