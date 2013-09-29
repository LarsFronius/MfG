#!/usr/bin/env python
import sys
import socket
import time
import subprocess
import ConfigParser
import logging
from optparse import OptionParser

import yaml

from munin import MuninClient

DEFAULT_CONFIG = {
		'config_file': '/etc/mfg.ini',
		'carbon_port': 2003,
		'metric_prefix': '{hostname}.',
		'interval': 60
		}

def facter():
    try:
        facter_output = subprocess.Popen(['facter','-py'], stdout=subprocess.PIPE, stderr=open("/dev/null", "w")).communicate()[0]
        y = yaml.load(facter_output)
        logging.debug('Got facts: %s', y)
        return y
    except OSError, e:
        logging.warning('Could not get facts: %s', e)
        return None

def compute_prefix(facts, prefix_pattern):
    """
    >>> prefix = 'servers.{datacenter}.{hostname}.'
    >>> facts = {'datacenter':'eu-west', 'hostname':'kellerautomat'}
    >>> compute_prefix(facts, prefix)
    'servers.eu-west.kellerautomat.'
    >>> prefix = '{hostname}.'
    >>> facts = None
    >>> compute_prefix(facts, prefix) == socket.gethostname() + '.'
    True
    """
    try:
        if facts:
            return prefix_pattern.format(**facts)
        else:
            return prefix_pattern.format(hostname=socket.gethostname())
    except KeyError, e:
        logging.error('not all facts in "%s" could be resolved: %s', prefix_pattern, e)
        sys.exit(1)

class CarbonClient(object):

    def __init__(self, host, port):
        self.host = host
        self.port = int(port)
        self._init_socket()

    def _init_socket(self):
        for res in socket.getaddrinfo(self.host, self.port, socket.AF_UNSPEC, socket.SOCK_STREAM):
            af, socktype, proto, cannonname, sa = res
            try:
                self.sock = socket.socket(af, socktype, proto)
            except socket.error as msg:
                s = None
                continue
            try: 
                self.sock.connect(sa)
            except IOError, e:
                raise e.__class__(e.errno, "%s:%s: %s" % (self.host, self.port, e.strerror))
            break

    def send(self, message):
        self.sock.sendall(message)


def parse_config_file(config_file):
    config_file_structure = (
            ('carbon_port', 'carbon', 'port'),
            ('carbon_host', 'carbon', 'host'),
            ('metric_prefix', 'mfg', 'prefix'),
            ('interval', 'mfg', 'interval'),
            )

    config = {}
    c = ConfigParser.ConfigParser()
    c.read(config_file)
    for config_key, section, key in config_file_structure:
        try:
            config[config_key] = c.get(section, key)
        except ConfigParser.Error:
            logging.debug('Could not get %s from config %s(section %s, key %s)',
                    config_key,
                    config_file,
                    section,
                    key)

    return config

def parse_command_line():
    # TODO
    parser = OptionParser()
    parser.add_option("-c", "--config", dest="config_file",
            help="use configuration in FILE, default: %s" % DEFAULT_CONFIG['config_file'], metavar="FILE")
    parser.add_option("-i", "--interval", dest="interval",
            help="send metrics every SECONDS, default: %s" % DEFAULT_CONFIG['interval'], metavar="SECONDS")
    parser.add_option("-H", "--carbon-host",
            help="send metrics to carbon host HOST", metavar="HOST")
    parser.add_option("-p", "--carbon-port",
            help="use carbon port PORT, default: %s" % DEFAULT_CONFIG['carbon_port'], metavar="PORT")
    parser.add_option("-m", "--metric-prefix",
            help="prefix every sent metric with PREFIX, default: %s" % DEFAULT_CONFIG['metric_prefix'], metavar="PREFIX")
    parser.add_option("-v", "--verbose", action="count", dest="verbose", help="be more verbose, may be used multiple times", default=0)

    (options, args) = parser.parse_args()
    logging.getLogger().setLevel((logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG)[min(options.verbose, 3)])
    logging.debug('command line options: %s', options)
    return dict((k,v) for k,v in options.__dict__.items() if v)

def fetch_from_munin(munin_client):
    logging.debug('going to ask munin for items')
    list_result = munin_client.list()
    logging.debug('munin: list command returned %d results', len(list_result))
    timestamp = int(time.time())
    messages = []
    for item in list_result:
        values = munin_client.fetch(item)
        for key in values:
            message = "%s.%s %s %d\n" % (item, key, values[key], timestamp)
            logging.debug('fetched from munin: %s', message)
            messages.append(message)

    return messages

def send_to_carbon(carbon_client, prefix, messages):
    prefixed_messages = [prefix + message for message in messages]
    carbon_client.send("".join(prefixed_messages))
    logging.info('sent %d messages', len(prefixed_messages))

def main():
    config_file = DEFAULT_CONFIG['config_file']
    command_line_config = parse_command_line()
    if 'config_file' in command_line_config:
        config_file = command_line_config['config_file']

    config_from_file = parse_config_file(config_file)

    config = DEFAULT_CONFIG.copy()
    config.update(config_from_file)
    config.update(command_line_config)
    logging.debug('merged config: %s', config)
    if 'carbon_host' not in config:
        logging.fatal('carbon_host not set, set in config file or use -H')
        raise RuntimeError()

    facts = facter()

    prefix = compute_prefix(facts, config['metric_prefix'])
    if not prefix.endswith('.'):
        prefix = prefix + '.'
    try:
        carbon_client = CarbonClient(config['carbon_host'], config['carbon_port'])
    except socket.error, e:
        print e
        sys.exit(1)

    munin_client = MuninClient('127.0.0.1')
    while True:
        try:
            munin_client = MuninClient('127.0.0.1')
            started = time.time()
            next_iteration = started + int(config['interval'])

            messages = fetch_from_munin(munin_client)
            send_to_carbon(carbon_client, prefix, messages)

            now = time.time()
            remaining_sleep = next_iteration - now
            if remaining_sleep > 0:
                logging.debug('sleeping %d', remaining_sleep)
                time.sleep(remaining_sleep)
            else:
                logging.warning('processing took %d seconds more than interval(%d), increase interval', -remaining_sleep, interval)

        except socket.error, e:
            print e
            sys.exit(1)

        except KeyboardInterrupt:
            munin_client.close()
            sys.exit(0)

if __name__ == '__main__':
    logging.root.name = 'mfg'
    logging.basicConfig(level=logging.DEBUG)
    main()
