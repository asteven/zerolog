# encoding: utf-8

import os
import socket
import json
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
stream_prefix = b'stream.'
config_prefix = b'config.'


# default endpoints
default_endpoints = {
    'control': 'tcp://127.0.0.42:6660',
    'collect': 'tcp://127.0.0.42:6661',
    'publish': 'tcp://127.0.0.42:6662',
}


def get_endpoint(endpoint):
    """
    Create directories for ipc endpoints as needed.
    """
    if endpoint.startswith('ipc://'):
        dirname = os.path.dirname(endpoint[6:])
        compat.makedirs(dirname, exist_ok=True)
    return endpoint


def get_endpoints_from_controller(endpoint):
    # TODO: error handling
    from zerolog.client import ZerologClient
    client = ZerologClient(endpoint=endpoint)
    message = client.call('endpoints')
    endpoints = message['response']
    return endpoints


def configure(endpoints=None, context=None):
    """Configure python logging to be zerolog aware.

    This replaces the logging.Logger.manager by one that knows how to get
    logger configs from a zerolog server.
    """
    manager = ZerologManager(
        endpoints or default_endpoints,
        context=context
    )
    manager.install()


def getLogger(name=None):
    """Get a zerolog enabled logger.

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


class ZerologManager(logging.Manager):
    def __init__(self, endpoints, context=None):
        super(ZerologManager, self).__init__(logging.getLogger())
        if 'control' in endpoints:
            self.endpoints = get_endpoints_from_controller(endpoints['control'])
        else:
            self.endpoints = endpoints
        self.context = context or zmq.Context.instance()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect(self.endpoints['publish'])
        self.log = logging.getLogger('zerolog')
        self.subscribed_loggers = []
        self.zerolog_handler = ZerologHandler(self.endpoints['collect'], context=self.context)
        #self.zerolog_handler = GreenZerologHandler(self.endpoints['collect'], context=self.context)

        # by default only the root logger get's a zerolog handler
        self.add_zerolog_handler(self.root)

        self._keep_going = True
        gevent.spawn(self.__configure)

    def __configure(self):
        """Main loop listening for config changes from the zerolog server.

        TODO: maybe create a thread based version of this to eliminate the
            dependency on gevent for clients.
            Will still need gevent to run the server.
        """
        while self._keep_going:
            topic, message = self.socket.recv_multipart()
            logger_name = topic[len(config_prefix):]
            config = json.loads(message.decode())
            logger = self.getLogger(logger_name)
            self.configure(logger, config)

    def configure(self, logger, config):
        if 'level' in config:
            level = config['level'].upper()
            self.log.debug('setting loglevel of {0} to {1}'.format(logger.name, level))
            logger.setLevel(level)
        if 'propagate' in config:
            propagate = config['propagate']
            if propagate:
                # if we are propagating to parent logger
                # there is no need for this logger to have it's
                # own zerolog handler, so remove it
                self.remove_zerolog_handler(logger)
            else:
                # otherwise it needs its own zerolog handler
                # or we simply wont get any logs from this handler
                self.add_zerolog_handler(logger)
            logger.propagate = propagate

    def add_zerolog_handler(self, logger):
        if not self.zerolog_handler in logger.handlers:
            self.log.debug('adding zerolog handler to {0}'.format(logger.name))
            logger.addHandler(self.zerolog_handler)

    def remove_zerolog_handler(self, logger):
        if self.zerolog_handler in logger.handlers:
            self.log.debug('removing zerolog handler from {0}'.format(logger.name))
            logger.removeHandler(self.zerolog_handler)

    def subscribe(self, name):
        """Subscribe to receive config updates for a logger.
        """
        if not isinstance(name, bytes):
            name = name.encode('ascii')
        if name not in self.subscribed_loggers:
            self.socket.setsockopt(zmq.SUBSCRIBE, config_prefix + name)
            self.subscribed_loggers.append(name)

    def getLogger(self, name):
        """Get a logger instance and subscribe to the zerolog server for
        config updates for it.
        """
        #self.log.debug('{0}.getLogger({1})'.format(self.__class__.__name__, name))
        logger = super(ZerologManager, self).getLogger(name)
        self.subscribe(name)
        return logger

    def install(self):
        """Install this manager as the global logging manager (logging.Logger.manager)
        """
        old_manager = logging.Logger.manager
        logging.Logger.manager = self
        self.inherit(old_manager)

    def inherit(self, other_manager):
        """Inherit attributes and logger instances from an other/existing
        manager.
        """
        for attr,value in other_manager.__dict__.items():
            if attr != 'loggerDict':
                setattr(self, attr, value)
        for name,logger in other_manager.loggerDict.items():
            self.loggerDict[name] = logger
            #if not isinstance(logger, logging.PlaceHolder):
            #    self.subscribe(name)
            self.subscribe(name)


class ZerologHandler(logging.Handler):
    """ A logging handler for sending notifications to a ømq endpoint.
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
        record_dict = record.__dict__
        # add hostname to record
        record_dict['hostname'] = self.hostname
        try:
            topic = '{0}{1}:{2}'.format(
                stream_prefix,
                record_dict['name'],
                record_dict['levelname']
            )
            self.socket.send_multipart([
                topic.encode('utf-8'),
                json.dumps(record_dict).encode('utf-8')
            ])
        except:
            self.handleError(record)


# FIXME: is there _any_ reason/benefit to use this instead of the other one?
#         probably not, probably nuke it
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
            record_dict = record.__dict__
            # add hostname to record
            record_dict['hostname'] = self.hostname
            try:
                topic = '{0}{1}:{2}'.format(
                    stream_prefix,
                    record_dict['name'],
                    record_dict['levelname']
                )
                self.socket.send_multipart([
                    topic.encode('utf-8'),
                    json.dumps(record_dict)
                ])
            except:
                self.handleError(record)
