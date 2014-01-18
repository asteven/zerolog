# encoding: utf-8
#

import sys
import argparse
import logging
import time

import gevent

import zmq.green as zmq
from zmq.utils.jsonapi import jsonmod as json

import zerolog
import zerolog.client


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_const', const='debug', dest='log_level',
        help='set log level to debug')
    parser.add_argument('-v', '--verbose', action='store_const', const='info', dest='log_level',
        help='be verbose, set log level to info')
    parser.add_argument('--endpoint', required=False,
        help='uri of the zeromq socket on which log messages are published')
    parser.add_argument('command', help='command to run')

    # TODO: use parse_known_args and let a command specific parser handle the rest

#    parser.add_argument('command_args', nargs='*', metavar='key:value',
#        help='zero or more %(metavar)s pairs as arguments for command')

    #args = parser.parse_args(argv)
    args, remaining_args = parser.parse_known_args(argv)
    log_level = args.log_level or 'info'
    level = getattr(logging, log_level.upper())
    log_format='%(levelname)s: %(name)s: %(message)s'
    logging.basicConfig(level=level, format=log_format, stream=sys.stderr)

    return args,remaining_args


def command_list(argv):
    log = logging.getLogger(__name__)
    client = zerolog.client.ZerologClient()
    message = client.call('list')
    log.debug('{0!r}'.format(message))
    for name,config in message['response'].items():
        print('{0}: {1}'.format(name, config))

def command_set(argv):
    log = logging.getLogger(__name__)
    parser = argparse.ArgumentParser()
    parser.add_argument('name', help='the name of the logger to set properties on')
    parser.add_argument('config', nargs='*', metavar='key:value',
        help='zero or more %(metavar)s pairs as config for logger')
    args = parser.parse_args(argv)
    config = dict(item.split(':') for item in args.config)
    payload = {
        'name': args.name,
        'config': config,
    }
    log.debug('payload: {0}'.format(payload))

    client = zerolog.client.ZerologClient()
    response = client.call('set', payload)
    log.debug('{0!r}'.format(response))

def command_get(argv):
    log = logging.getLogger(__name__)
    parser = argparse.ArgumentParser()
    parser.add_argument('name', help='the name of the logger to get properties for')
    args = parser.parse_args(argv)
    payload = {
        'name': args.name,
    }
    log.debug('payload: {0}'.format(payload))

    client = zerolog.client.ZerologClient()
    message = client.call('get', payload)
    log.debug('{0!r}'.format(message))
    for name,config in message['response'].items():
        print('{0}: {1}'.format(name, config))


def main(argv=sys.argv[1:]):
    args,command_args = parse_args(argv)
    log = logging.getLogger(__name__)
    log.debug('args: {0}'.format(args))
    log.debug('command_args: {0}'.format(command_args))

    command = globals().get('command_{0}'.format(args.command), None)
    if command:
        command(command_args)
    else:
        print('unknown command: {0}'.format(args.command))
    return

    client = zerolog.client.ZerologClient()
    command_args = dict(item.split(':') for item in args.command_args)
    log.debug('command_args: {0}'.format(command_args))
    response = client.call(args.command, command_args)
    print('{0!r}'.format(response))
    return

    client = zerolog.client.ZerologClient()
    response = client.call('list')
    print('{0!r}'.format(response))

    request = {'name': '__main__', 'config': {'level': 'ERROR'}}
    response = client.call('set', request)
    print('{0!r}'.format(response))

    time.sleep(5)

    request = {'name': '__main__', 'config': {'level': 'DEBUG'}}
    response = client.call('set', request)
    print('{0!r}'.format(response))

    time.sleep(5)

    request = {'name': '__main__', 'config': {'level': 'ERROR'}}
    response = client.call('set', request)
    print('{0!r}'.format(response))


if __name__ == '__main__':
    main()
