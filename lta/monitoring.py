"""Module to implement the monitoring component of the Long Term Archive."""

import asyncio
import logging
import time
import sys

from rest_tools.client import RestClient

from .config import from_environment
from .log_format import StructuredFormatter

EXPECTED_CONFIG = [
    'LTA_REST_URL',
    'LTA_REST_TOKEN',
]
EXPECTED_CONFIG_DEFAULTS = {
    'ENABLE_PROMETHEUS': 'false',
    'PROMETHEUS_MONITORING_INTERVAL': '60',  # seconds
    'PROMETHEUS_PORT': '8000',
}

class Monitor:
    """
    Generic monitor class

    Args:
        rest_url (str): url to LTA REST API
        token (str): token for LTA REST API
        interval (int): time interval between monitoring points
    """
    def __init__(self, lta_rest_url, lta_rest_token, monitoring_interval=60, logger=None):
        self.logger = logger if logger else logging
        self.interval = int(monitoring_interval)
        self.rest = RestClient(lta_rest_url, lta_rest_token,
                               timeout=self.interval//10, retries=1)
        self.running = True

    async def get_from_rest(self):
        """
        Get status data from LTA REST API.

        Returns:
            dict: overall health, and health of each component
        """
        self.logger.info('make REST API /status request')
        return await self.rest.request('GET', '/status')

    async def do(self):
        """
        Actually do the monitoring.

        This is for subclasses to implement.
        """
        raise NotImplementedError()

    async def run(self):
        """Run in a loop, calling `do`."""
        while self.running:
            start = time.time()
            await self.do()
            sleep_time = max(0.1, self.interval - (time.time() - start))
            await asyncio.sleep(sleep_time)

    def stop(self):
        """Stop a monitor's `run` loop."""
        self.running = False

MONITOR_NAMES = {}

try:
    from prometheus_client import start_http_server, Info, Enum
except ImportError:
    pass
else:
    class PrometheusMonitor(Monitor):
        def __init__(self, port=8000, **kwargs):
            super(PrometheusMonitor, self).__init__(**kwargs)
            start_http_server(int(port))
            self.state = {}

        def register_enum(self, name):
            """Register enum"""
            desc = 'Health of '+name
            if name == 'health':
                desc = 'Overall LTA health'
            
            self.state[name] = Enum(name, desc,
                                    states=['OK', 'WARN', 'ERROR'])

        async def do(self):
            self.logger.info('do Prometheus monitor')
            ret = await self.get_from_rest()
            for n in ret:
                if n not in self.state:
                    self.register_enum(n)
                self.state[n].state(ret[n])

    MONITOR_NAMES['PROMETHEUS'] = PrometheusMonitor


def check_bool(text):
    """Check if a string is bool-like and return that"""
    text = text.lower()
    if text in ('true', 't', '1', 'yes', 'y', 'on'):
        return True
    else:
        return False

def main() -> None:
    """Configure a monitoring component from the environment and set it running."""
    config = from_environment(EXPECTED_CONFIG)
    config.update(from_environment(EXPECTED_CONFIG_DEFAULTS))

    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='Monitoring',
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.monitoring")

    monitors = []
    loop = asyncio.get_event_loop()
    for name in MONITOR_NAMES:
        if check_bool(config['ENABLE_'+name]):
            logger.info(f"Setting up monitor {name}")
            kwargs = {n.split('_', 1)[-1].lower(): config[n] for n in config if n.startswith(name)}
            kwargs.update({
                'lta_rest_url': config['LTA_REST_URL'],
                'lta_rest_token': config['LTA_REST_TOKEN'],
            })
            m = MONITOR_NAMES[name](**kwargs)
            monitors.append(m)
            loop.create_task(m.run())

    logger.info("Starting asyncio loop")
    loop.run_forever()

if __name__ == "__main__":
    main()
