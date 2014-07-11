# encoding: utf-8
#

import sys
import argparse
import logging
import time
import json

import gevent

import zmq.green as zmq

import zerolog
import zerolog.client


class ControllerClient(zerolog.client.ZerologClient):
    def command_endpoints(self, argv):
        log = logging.getLogger(__name__)
        message = self.call('endpoints')
        log.debug('{0!r}'.format(message))
        for name,config in message['response'].items():
            print('{0}: {1}'.format(name, config))


    def command_list(self, argv):
        log = logging.getLogger(__name__)
        message = self.call('list')
        log.debug('{0!r}'.format(message))
        for name,config in message['response'].items():
            print('{0}: {1}'.format(name, config))


    def command_set(self, argv):
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

        response = self.call('set', payload)
        log.debug('{0!r}'.format(response))


    def command_get(self, argv):
        log = logging.getLogger(__name__)
        parser = argparse.ArgumentParser()
        parser.add_argument('name', help='the name of the logger to get properties for')
        args = parser.parse_args(argv)
        payload = {
            'name': args.name,
        }
        log.debug('payload: {0}'.format(payload))

        message = self.call('get', payload)
        log.debug('{0!r}'.format(message))
        for name,config in message['response'].items():
            print('{0}: {1}'.format(name, config))

    def run(self, command, command_args):
        command = getattr(self, 'command_{0}'.format(command), None)
        if command:
            command(command_args)
        else:
            print('unknown command: {0}'.format(command))
            sys.exit(1)


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_const', const='debug', dest='log_level',
        help='set log level to debug')
    parser.add_argument('-v', '--verbose', action='store_const', const='info', dest='log_level',
        help='be verbose, set log level to info')
    parser.add_argument('--endpoint', required=False,
        help='uri of the zerolog control socket',
        default=zerolog.default_endpoints['control'])
    parser.add_argument('command', help='command to run')

    args, remaining_args = parser.parse_known_args(argv)
    log_level = args.log_level or 'info'
    level = getattr(logging, log_level.upper())
    log_format='%(levelname)s: %(name)s: %(message)s'
    logging.basicConfig(level=level, format=log_format, stream=sys.stderr)

    return args,remaining_args


def main(argv=sys.argv[1:]):
    args,command_args = parse_args(argv)
    log = logging.getLogger(__name__)
    log.debug('args: {0}'.format(args))
    log.debug('command_args: {0}'.format(command_args))

    client = ControllerClient(endpoint=zerolog.get_endpoint(args.endpoint))

    try:
        client.run(args.command, command_args)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
