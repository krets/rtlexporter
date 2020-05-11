#!/usr/bin/env python
import json
import logging
import sys
import time
from datetime import datetime, timedelta

import requests
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, REGISTRY

LOG = logging.getLogger('rtlexporter')


class RtlCollector(object):
    """ rtl_433 exporter

    Collects all found devices and exposes them as metrics

    """
    def __init__(self, url=None):
        """
        Args:
            url: optional for testing purposes
        """
        self.url = url
        self.data = {
            'temperature': {},
            'battery': {},
            'humidity': {}
        }

        self.labels = ['model', 'id', 'channel']


    def collect(self):
        """ Build a metric for all entries in self.data """
        for name, data in self.data.items():
            metric = GaugeMetricFamily(
                'rtl_433_%s' % name,
                '%s from the rtl_433 command' % name,
                labels=self.labels)

            for labels, value in data.items():
                metric.add_metric(labels, value)

            yield metric

    def read_http(self):
        """ Test reader for remote access to rtl_433 log

        Yields: json blob
        """
        while True:
            lines = [_ for _ in requests.get(self.url).content.decode().split("\n") if _]
            yield lines[-1]
            for _ in range(6000):
                time.sleep(.01)

    def read_stdin(self):
        """ reads stdin piped from rtl_433

        Yields: json blob

        """
        k = 0
        buff = ''
        while True:
            buff += sys.stdin.read(1)
            if buff.endswith('\n'):
                yield buff
                buff = ''

    def run(self):
        """ Endlessly collect data from either URL or stdin

        """
        func = self.read_stdin
        if self.url is not None:
            func = self.read_http
        try:
            for line in func():
                try:
                    data = json.loads(line)
                except ValueError:
                    LOG.error("can't decode json: %s" % line)
                    continue

                LOG.debug(data)
                if 'time' in data:
                    sample_time = datetime.strptime(data['time'], '%Y-%m-%d %H:%M:%S')
                    if (datetime.utcnow() - sample_time) > timedelta(seconds=120):
                        LOG.warning("Out of date sample: %s", data['time'])
                key = tuple([str(data.get(_)) for _ in self.labels])
                self.data['temperature'][key] = data.get('temperature_C', -40.0)
                self.data['battery'][key] = data.get('battery_ok', 0)
                self.data['humidity'][key] = data.get('humidity', 0)

        except KeyboardInterrupt:
            sys.stdout.flush()


if __name__ == "__main__":
    url = None
    if len(sys.argv) > 1:
        url = sys.argv[1]
    collector = RtlCollector(url=url)
    REGISTRY.register(collector)
    start_http_server(9118)
    collector.run()
