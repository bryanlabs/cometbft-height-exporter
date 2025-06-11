import time
import yaml
import logging
import requests
from prometheus_client import start_http_server, Gauge, Info
from threading import Thread
from statistics import mean
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
import sys

try:
    from pythonjsonlogger import jsonlogger
except ImportError:
    jsonlogger = None

logger = logging.getLogger("cometbft_height_exporter")

def setup_logging(log_format='text', log_level='INFO'):
    level = getattr(logging, log_level.upper(), logging.INFO)
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    if log_format == 'json':
        if jsonlogger is not None:
            fmt = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(name)s %(message)s')
            handler.setFormatter(fmt)
        else:
            fmt = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
            handler.setFormatter(fmt)
            root_logger.warning("python-json-logger not installed, falling back to text logging.")
    else:
        fmt = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
        handler.setFormatter(fmt)
    # Remove all handlers associated with the root logger object.
    for h in root_logger.handlers[:]:
        root_logger.removeHandler(h)
    root_logger.addHandler(handler)


# Config loader
def load_config(path):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

# Metrics
HEIGHT_GAUGE = Gauge(
    'blockchain_latest_block_height',
    'Latest block height reported by reference nodes',
    ['node', 'network']
)
CATCHING_UP_GAUGE = Gauge(
    'blockchain_sync_catching_up',
    'Whether the node is catching up (1=true, 0=false)',
    ['node', 'network']
)
BLOCK_TIME_GAUGE = Gauge(
    'blockchain_latest_block_time',
    'Latest block time as unix timestamp',
    ['node', 'network']
)
# NETWORK_INFO = Info(
#     'blockchain_network_info',
#     'Network (chain-id) for the node',
#     ['network']
# )

# Polling logic
def fetch_status(url):
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        # Log full response JSON if debugging
        if logger.isEnabledFor(logging.DEBUG):
            try:
                logger.debug({
                    'event': 'node_response',
                    'url': url,
                    'response': resp.json()
                })
            except Exception as e:
                logger.debug({'event': 'node_response', 'url': url, 'error': f'Failed to parse JSON: {e}'})
        data = resp.json().get('result', {})
        node_info = data.get('node_info', {})
        sync_info = data.get('sync_info', {})
        network = node_info.get('network', 'unknown')
        height = int(sync_info.get('latest_block_height', 0))
        catching_up = int(sync_info.get('catching_up', False))
        block_time = sync_info.get('latest_block_time')
        if block_time:
            # Convert ISO8601 to unix timestamp
            try:
                from dateutil import parser
                ts = int(parser.isoparse(block_time).timestamp())
            except Exception:
                ts = 0
        else:
            ts = 0
        return {
            'network': network,
            'height': height,
            'catching_up': catching_up,
            'block_time': ts,
            'node_info': node_info,
        }
    except Exception as e:
        logger.warning(f"Failed to fetch status from {url}: {e}")
        return None

# Aggregation gauges (created once, reused)
AGGREGATION_GAUGES = {
    'max': Gauge('blockchain_latest_block_height_max', 'max block height across reference nodes', ['network']),
    'mean': Gauge('blockchain_latest_block_height_mean', 'mean block height across reference nodes', ['network'])
}

def update_metrics(nodes, aggregation, chain_id):
    successes = 0
    failures = 0
    errors = []
    heights = []
    for url in nodes:
        try:
            status = fetch_status(url + '/status')
            if not status:
                failures += 1
                errors.append((url, 'No status returned'))
                continue
            HEIGHT_GAUGE.labels(node=url, network=chain_id).set(status['height'])
            CATCHING_UP_GAUGE.labels(node=url, network=chain_id).set(status['catching_up'])
            BLOCK_TIME_GAUGE.labels(node=url, network=chain_id).set(status['block_time'])
            heights.append(status['height'])
            successes += 1
        except Exception as e:
            failures += 1
            errors.append((url, str(e)))
    # Aggregate height metrics per network
    if heights and aggregation in AGGREGATION_GAUGES:
        if aggregation == 'max':
            agg_val = max(heights)
        elif aggregation == 'mean':
            agg_val = mean(heights)
        else:
            agg_val = max(heights)
        AGGREGATION_GAUGES[aggregation].labels(network=chain_id).set(agg_val)
    logger.info(f"Scraped chain {chain_id}: {successes} successful, {failures} failed.")
    if failures:
        for url, err in errors:
            logger.error(f"Error scraping {chain_id} node {url}: {err}")

def poller(config):
    chains = config['chains']
    aggregation = config.get('aggregation', 'max')
    interval = config.get('interval', 15)
    while True:
        for chain_id, nodes in chains.items():
            update_metrics(nodes, aggregation, chain_id)
        time.sleep(interval)

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

# ... rest of imports and code ...

def run_healthz_server(port=8001):
    class HealthzHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/healthz':
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b'ok')
            else:
                self.send_response(404)
                self.end_headers()
        def log_message(self, format, *args):
            return  # Silence default logging
    server = HTTPServer(('0.0.0.0', port), HealthzHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()

def main():
    import argparse
    parser = argparse.ArgumentParser(description='CometBFT Prometheus Height Exporter')
    parser.add_argument('--config', default='config.yaml')
    parser.add_argument('--port', type=int, default=8000)
    parser.add_argument('--healthz-port', type=int, default=8001)
    args = parser.parse_args()
    config = load_config(args.config)
    # Logging config
    log_format = config.get('log_format', 'text')
    log_level = config.get('log_level', 'INFO')
    setup_logging(log_format, log_level)
    logger.info(f"Loaded config: {config}")
    start_http_server(args.port)
    logger.info(f"Exporter running on :{args.port}/metrics")
    run_healthz_server(args.healthz_port)
    logger.info(f"Health endpoint running on :{args.healthz_port}/healthz")
    poller(config)

if __name__ == "__main__":
    main()
