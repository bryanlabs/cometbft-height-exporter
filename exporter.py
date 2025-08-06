import time
import yaml
import logging
import requests
from prometheus_client import start_http_server, Gauge, Info, Counter
from threading import Thread
from statistics import mean
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
import sys
from typing import Dict, List, Optional, Tuple

try:
    from pythonjsonlogger import jsonlogger
except ImportError:
    jsonlogger = None

logger = logging.getLogger("cometbft_height_exporter")


def setup_logging(log_format="text", log_level="INFO"):
    level = getattr(logging, log_level.upper(), logging.INFO)
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    if log_format == "json":
        if jsonlogger is not None:
            fmt = jsonlogger.JsonFormatter(
                "%(asctime)s %(levelname)s %(name)s %(message)s"
            )
            handler.setFormatter(fmt)
        else:
            fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
            handler.setFormatter(fmt)
            root_logger.warning(
                "python-json-logger not installed, falling back to text logging."
            )
    else:
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        handler.setFormatter(fmt)
    # Remove all handlers associated with the root logger object.
    for h in root_logger.handlers[:]:
        root_logger.removeHandler(h)
    root_logger.addHandler(handler)


# Config loader with normalization
def load_config(path):
    with open(path, "r") as f:
        raw_config = yaml.safe_load(f)
    return normalize_config(raw_config)


def normalize_config(config):
    """
    Normalize config to support both old (simple list) and new (dict with cname) formats.

    Old format:
      chains:
        cosmoshub-4:
          - https://rpc1.com
          - https://rpc2.com

    New format:
      chains:
        cosmoshub-4:
          cname: cosmoshub
          interval: 15
          endpoints:
            - url: https://rpc1.com
              interval: 5
            - https://rpc2.com
    """
    normalized = {
        "chains": {},
        "interval": config.get("interval", 15),
        "aggregation": config.get("aggregation", "max"),
        "log_format": config.get("log_format", "json"),
        "log_level": config.get("log_level", "INFO"),
        "registry": config.get(
            "registry",
            {
                "update_interval": 3600,
                "health_check_timeout": 5,
                "min_healthy_threshold": 3,
            },
        ),
    }

    for chain_id, chain_config in config["chains"].items():
        if isinstance(chain_config, list):
            # Old format - simple list of URLs
            normalized["chains"][chain_id] = {
                "cname": None,  # No registry integration for old format
                "interval": normalized["interval"],
                "use_chain_registry": False,
                "max_registry_rpcs": 0,
                "endpoints": [
                    {"url": url, "interval": normalized["interval"]}
                    for url in chain_config
                ],
            }
        else:
            # New format - dict with cname, intervals, etc.
            endpoints = []
            for endpoint in chain_config.get("endpoints", []):
                if isinstance(endpoint, str):
                    # Simple URL string
                    endpoints.append(
                        {
                            "url": endpoint,
                            "interval": chain_config.get(
                                "interval", normalized["interval"]
                            ),
                        }
                    )
                else:
                    # Dict with url and optional interval
                    endpoints.append(
                        {
                            "url": endpoint["url"],
                            "interval": endpoint.get(
                                "interval",
                                chain_config.get("interval", normalized["interval"]),
                            ),
                        }
                    )

            normalized["chains"][chain_id] = {
                "cname": chain_config.get("cname"),
                "interval": chain_config.get("interval", normalized["interval"]),
                "use_chain_registry": chain_config.get("use_chain_registry", False),
                "max_registry_rpcs": chain_config.get("max_registry_rpcs", 5),
                "endpoints": endpoints,
            }

    return normalized


class ChainRegistryClient:
    """
    Client for fetching RPC endpoints from cosmos chain-registry.
    Handles both mainnet and testnet chains with proper validation.
    """

    REGISTRY_BASE_URL = "https://raw.githubusercontent.com/cosmos/chain-registry/master"

    def __init__(self, health_check_timeout=5):
        self.health_check_timeout = health_check_timeout
        self.session = requests.Session()
        # Cache for fetched chain data to avoid repeated requests
        self._chain_cache = {}

    def fetch_chain_rpcs(self, cname: str, expected_chain_id: str) -> List[str]:
        """
        Fetch RPC endpoints for a given canonical name and validate chain_id.

        Args:
            cname: Canonical name (e.g., "cosmoshub", "akashtestnet")
            expected_chain_id: Expected chain ID for validation

        Returns:
            List of RPC URLs that pass health checks
        """
        if cname in self._chain_cache:
            cached_data, timestamp = self._chain_cache[cname]
            # Use cache for 1 hour
            if time.time() - timestamp < 3600:
                return self._extract_healthy_rpcs(cached_data, expected_chain_id)

        # Try mainnet path first, then testnet
        paths_to_try = [f"{cname}/chain.json", f"testnets/{cname}/chain.json"]

        for path in paths_to_try:
            try:
                url = f"{self.REGISTRY_BASE_URL}/{path}"
                logger.debug(f"Fetching chain registry: {url}")

                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                chain_data = response.json()

                # Validate chain_id matches
                if chain_data.get("chain_id") != expected_chain_id:
                    logger.warning(
                        f"Chain ID mismatch for {cname}: got {chain_data.get('chain_id')}, "
                        f"expected {expected_chain_id}"
                    )
                    continue

                # Cache the successful result
                self._chain_cache[cname] = (chain_data, time.time())

                logger.info(f"Found chain registry data: {cname} -> {path}")
                return self._extract_healthy_rpcs(chain_data, expected_chain_id)

            except requests.RequestException as e:
                logger.debug(f"Failed to fetch {path}: {e}")
                continue
            except (KeyError, ValueError) as e:
                logger.warning(f"Invalid chain.json format in {path}: {e}")
                continue

        logger.warning(
            f"No chain registry data found for {cname} (chain_id: {expected_chain_id})"
        )
        return []

    def _extract_healthy_rpcs(self, chain_data: dict, chain_id: str) -> List[str]:
        """Extract RPC endpoints and filter by health checks."""
        rpcs = []

        # Extract RPC endpoints from apis.rpc array
        apis = chain_data.get("apis", {})
        rpc_configs = apis.get("rpc", [])

        for rpc_config in rpc_configs:
            address = rpc_config.get("address", "")
            if address and address.startswith(("http://", "https://")):
                rpcs.append(address.rstrip("/"))

        logger.debug(
            f"Found {len(rpcs)} RPC endpoints for {chain_id} in chain registry"
        )

        # Health check the RPCs
        healthy_rpcs = []
        for rpc_url in rpcs:
            if self._health_check_rpc(rpc_url, chain_id):
                healthy_rpcs.append(rpc_url)

        logger.info(
            f"Chain registry RPCs for {chain_id}: {len(healthy_rpcs)}/{len(rpcs)} healthy"
        )
        return healthy_rpcs

    def _health_check_rpc(self, rpc_url: str, expected_chain_id: str) -> bool:
        """Perform basic health check on RPC endpoint."""
        try:
            status_url = f"{rpc_url}/status"
            response = self.session.get(status_url, timeout=self.health_check_timeout)
            response.raise_for_status()

            data = response.json().get("result", {})
            node_info = data.get("node_info", {})
            actual_chain_id = node_info.get("network", "")

            if actual_chain_id != expected_chain_id:
                logger.debug(
                    f"RPC {rpc_url} chain_id mismatch: {actual_chain_id} != {expected_chain_id}"
                )
                return False

            return True

        except Exception as e:
            logger.debug(f"RPC health check failed for {rpc_url}: {e}")
            return False


# Metrics
HEIGHT_GAUGE = Gauge(
    "blockchain_latest_block_height",
    "Latest block height reported by reference nodes",
    ["node", "network"],
)
CATCHING_UP_GAUGE = Gauge(
    "blockchain_sync_catching_up",
    "Whether the node is catching up (1=true, 0=false)",
    ["node", "network"],
)
BLOCK_TIME_GAUGE = Gauge(
    "blockchain_latest_block_time",
    "Latest block time as unix timestamp",
    ["node", "network"],
)

# New enhanced metrics
RPC_RESPONSE_TIME_GAUGE = Gauge(
    "blockchain_rpc_response_time_seconds",
    "Response time for RPC endpoint requests",
    ["node", "network"],
)
CHAIN_HEIGHT_DRIFT_GAUGE = Gauge(
    "blockchain_chain_height_drift_seconds",
    "Maximum height drift across all RPCs in a chain (in block time seconds)",
    ["network"],
)
HEALTHY_ENDPOINTS_GAUGE = Gauge(
    "blockchain_healthy_endpoints_count",
    "Number of healthy RPC endpoints per chain",
    ["network"],
)
REGISTRY_ENDPOINTS_GAUGE = Gauge(
    "blockchain_registry_endpoints_count",
    "Number of endpoints discovered from chain registry",
    ["network"],
)
# Enhanced metrics for adaptive system
RPC_CURRENT_INTERVAL_GAUGE = Gauge(
    "blockchain_rpc_current_interval_seconds",
    "Current adaptive polling interval for each endpoint",
    ["node", "network"],
)
RPC_TARGET_INTERVAL_GAUGE = Gauge(
    "blockchain_rpc_target_interval_seconds",
    "Desired polling interval for each endpoint",
    ["node", "network"],
)
RPC_POLLING_EFFICIENCY_GAUGE = Gauge(
    "blockchain_rpc_polling_efficiency_percent",
    "Efficiency percentage (target_interval/current_interval * 100)",
    ["node", "network"],
)
ACTUAL_POLLING_FREQUENCY_GAUGE = Gauge(
    "blockchain_actual_polling_frequency_hz",
    "Actual polling frequency achieved (1/current_interval)",
    ["node", "network"],
)
CONSECUTIVE_429S_GAUGE = Gauge(
    "blockchain_consecutive_429s_count",
    "Number of consecutive 429 errors for endpoint",
    ["node", "network"],
)
DEGRADED_ENDPOINTS_GAUGE = Gauge(
    "blockchain_degraded_endpoints_count",
    "Endpoints running slower than desired",
    ["network", "severity"],  # "moderate", "severe"
)
CHAIN_EFFICIENCY_GAUGE = Gauge(
    "blockchain_chain_efficiency_percent",
    "Average polling efficiency across all endpoints in chain",
    ["network"],
)
CHAIN_CONFIDENCE_GAUGE = Gauge(
    "blockchain_chain_confidence_percent",
    "Confidence in the reported max block height (0-100%)",
    ["network"],
)
INTERVAL_ADJUSTMENT_COUNTER = Counter(
    "blockchain_interval_adjustments_total",
    "Total number of interval adjustments (backoffs + improvements)",
    ["node", "network", "direction"],  # "increase" or "decrease"
)


# Polling logic
def fetch_status(url):
    """Fetch status from RPC endpoint with timing."""
    start_time = time.time()
    try:
        resp = requests.get(url, timeout=5)
        response_time = time.time() - start_time
        resp.raise_for_status()

        # Log full response JSON if debugging
        if logger.isEnabledFor(logging.DEBUG):
            try:
                logger.debug(
                    {"event": "node_response", "url": url, "response": resp.json()}
                )
            except Exception as e:
                logger.debug(
                    {
                        "event": "node_response",
                        "url": url,
                        "error": f"Failed to parse JSON: {e}",
                    }
                )
        data = resp.json().get("result", {})
        node_info = data.get("node_info", {})
        sync_info = data.get("sync_info", {})
        network = node_info.get("network", "unknown")
        height = int(sync_info.get("latest_block_height", 0))
        catching_up = int(sync_info.get("catching_up", False))
        block_time = sync_info.get("latest_block_time")
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
            "network": network,
            "height": height,
            "catching_up": catching_up,
            "block_time": ts,
            "response_time": response_time,
            "node_info": node_info,
        }
    except Exception as e:
        response_time = time.time() - start_time
        logger.warning(f"Failed to fetch status from {url}: {e}")
        return {"error": str(e), "response_time": response_time}


class AdaptiveInterval:
    """TCP-like adaptive interval management for RPC endpoints."""

    def __init__(
        self,
        desired_interval: float,
        is_manual: bool = False,
        url: str = "",
        chain_id: str = "",
    ):
        self.desired_interval = desired_interval  # User's configured interval
        self.current_interval = desired_interval  # Current adaptive interval
        self.is_manual = is_manual
        self.url = url  # For metrics labeling
        self.chain_id = chain_id  # For metrics labeling

        # Success tracking for gradual improvement
        self.consecutive_successes = 0
        self.success_threshold = (
            15 if is_manual else 10
        )  # Manual endpoints are more conservative

        # 429 tracking
        self.consecutive_429s = 0
        self.total_429s = 0
        self.last_429_time = 0

        # Other error tracking
        self.consecutive_errors = 0
        self.total_errors = 0

        # Bounds
        self.max_interval = 300  # 5 minutes max
        self.backoff_multiplier = 1.5
        self.improvement_factor = 0.85  # 15% improvement when stable

    def on_success(self):
        """Record successful request and potentially improve interval."""
        self.consecutive_successes += 1
        self.consecutive_429s = 0
        self.consecutive_errors = 0

        # Gradually approach desired interval when stable
        if self.consecutive_successes >= self.success_threshold:
            if self.current_interval > self.desired_interval:
                # Improve towards desired interval, but not below it
                new_interval = max(
                    self.desired_interval,
                    self.current_interval * self.improvement_factor,
                )

                # Log improvements for visibility
                if new_interval < self.current_interval:
                    improvement = (
                        (self.current_interval - new_interval) / self.current_interval
                    ) * 100
                    logger.debug(
                        f"Improving interval for {self.url}: {self.current_interval:.1f}s → {new_interval:.1f}s (-{improvement:.1f}%)"
                    )
                    # Track improvement
                    INTERVAL_ADJUSTMENT_COUNTER.labels(
                        node=self.url, network=self.chain_id, direction="decrease"
                    ).inc()

                self.current_interval = new_interval

            self.consecutive_successes = 0

    def on_429(self):
        """Record 429 error and apply exponential backoff."""
        self.consecutive_429s += 1
        self.total_429s += 1
        self.consecutive_successes = 0
        self.last_429_time = time.time()

        # Exponential backoff
        old_interval = self.current_interval
        self.current_interval = min(
            self.max_interval, self.current_interval * self.backoff_multiplier
        )

        # Log backoff for visibility and track metric
        backoff_pct = ((self.current_interval - old_interval) / old_interval) * 100
        logger.info(
            f"429 backoff {self.url}: {old_interval:.1f}s → {self.current_interval:.1f}s (+{backoff_pct:.1f}%)"
        )
        INTERVAL_ADJUSTMENT_COUNTER.labels(
            node=self.url, network=self.chain_id, direction="increase"
        ).inc()

    def on_error(self):
        """Record non-429 error and apply moderate backoff."""
        self.consecutive_errors += 1
        self.total_errors += 1
        self.consecutive_successes = 0

        # Moderate backoff for non-429 errors (less aggressive)
        old_interval = self.current_interval
        self.current_interval = min(self.max_interval, self.current_interval * 1.2)

        if self.current_interval > old_interval:
            logger.debug(
                f"Error backoff {self.url}: {old_interval:.1f}s → {self.current_interval:.1f}s"
            )
            INTERVAL_ADJUSTMENT_COUNTER.labels(
                node=self.url, network=self.chain_id, direction="increase"
            ).inc()

    def get_status(self) -> dict:
        """Get current status for monitoring."""
        efficiency = (
            (self.desired_interval / self.current_interval) * 100
            if self.current_interval > 0
            else 0
        )
        return {
            "desired_interval": self.desired_interval,
            "current_interval": self.current_interval,
            "efficiency_pct": efficiency,
            "consecutive_successes": self.consecutive_successes,
            "consecutive_429s": self.consecutive_429s,
            "total_429s": self.total_429s,
            "total_errors": self.total_errors,
            "is_improving": self.current_interval > self.desired_interval,
        }


class EndpointPoller:
    """Individual endpoint poller with adaptive interval management."""

    def __init__(
        self,
        url: str,
        chain_id: str,
        interval: int,
        aggregation: str,
        is_manual: bool = False,
    ):
        self.url = url
        self.chain_id = chain_id
        self.aggregation = aggregation
        self.is_manual = is_manual
        self.thread = None
        self.running = False
        self.last_result = None

        # Initialize adaptive interval management
        self.adaptive_interval = AdaptiveInterval(
            float(interval), is_manual, url, chain_id
        )

    def start(self, stagger_delay: float = 0):
        """Start polling with optional stagger delay."""
        self.running = True
        self.thread = threading.Thread(
            target=self._poll_loop, args=(stagger_delay,), daemon=True
        )
        self.thread.start()

    def stop(self):
        """Stop polling."""
        self.running = False
        if self.thread:
            self.thread.join()

    def _poll_loop(self, stagger_delay: float):
        """Main polling loop with adaptive interval management."""
        # Initial stagger delay
        if stagger_delay > 0:
            logger.debug(f"Staggering start for {self.url} by {stagger_delay}s")
            time.sleep(stagger_delay)

        while self.running:
            try:
                status = fetch_status(self.url + "/status")
                self.last_result = status

                if status and "error" not in status:
                    # Success - update adaptive interval and metrics
                    self.adaptive_interval.on_success()

                    HEIGHT_GAUGE.labels(node=self.url, network=self.chain_id).set(
                        status["height"]
                    )
                    CATCHING_UP_GAUGE.labels(node=self.url, network=self.chain_id).set(
                        status["catching_up"]
                    )
                    BLOCK_TIME_GAUGE.labels(node=self.url, network=self.chain_id).set(
                        status["block_time"]
                    )
                    RPC_RESPONSE_TIME_GAUGE.labels(
                        node=self.url, network=self.chain_id
                    ).set(status["response_time"])

                    # Update enhanced adaptive interval metrics
                    adaptive_status = self.adaptive_interval.get_status()
                    RPC_CURRENT_INTERVAL_GAUGE.labels(
                        node=self.url, network=self.chain_id
                    ).set(adaptive_status["current_interval"])
                    RPC_TARGET_INTERVAL_GAUGE.labels(
                        node=self.url, network=self.chain_id
                    ).set(adaptive_status["desired_interval"])
                    RPC_POLLING_EFFICIENCY_GAUGE.labels(
                        node=self.url, network=self.chain_id
                    ).set(adaptive_status["efficiency_pct"])
                    ACTUAL_POLLING_FREQUENCY_GAUGE.labels(
                        node=self.url, network=self.chain_id
                    ).set(1.0 / adaptive_status["current_interval"])
                    CONSECUTIVE_429S_GAUGE.labels(
                        node=self.url, network=self.chain_id
                    ).set(adaptive_status["consecutive_429s"])
                else:
                    # Handle different types of errors
                    error_msg = (
                        status.get("error", "Unknown error")
                        if status
                        else "No response"
                    )
                    self._handle_adaptive_error(error_msg)

            except Exception as e:
                logger.error(f"Error in polling loop for {self.url}: {e}")
                self._handle_adaptive_error(str(e))

            # Use adaptive interval for next poll
            time.sleep(self.adaptive_interval.current_interval)

    def _handle_adaptive_error(self, error_msg: str):
        """Handle errors with adaptive interval adjustment."""
        is_429 = "429" in error_msg or "Too Many Requests" in error_msg

        if is_429:
            self.adaptive_interval.on_429()
            # Only log first few 429s to avoid spam
            if self.adaptive_interval.consecutive_429s <= 3:
                logger.warning(f"Rate limited {self.url}: {error_msg}")
        else:
            self.adaptive_interval.on_error()
            logger.warning(f"Failed to poll {self.url}: {error_msg}")

    def get_status_summary(self) -> dict:
        """Get current status summary for monitoring."""
        adaptive_status = self.adaptive_interval.get_status()
        return {
            "url": self.url,
            "is_manual": self.is_manual,
            "desired_interval": adaptive_status["desired_interval"],
            "current_interval": adaptive_status["current_interval"],
            "efficiency_pct": adaptive_status["efficiency_pct"],
            "total_429s": adaptive_status["total_429s"],
            "total_errors": adaptive_status["total_errors"],
            "is_improving": adaptive_status["is_improving"],
            "last_success": self.last_result is not None
            and "error" not in (self.last_result or {}),
        }


class ChainManager:
    """Manages all endpoints for a single chain with health monitoring."""

    def __init__(
        self,
        chain_id: str,
        chain_config: dict,
        registry_client: ChainRegistryClient,
        aggregation: str,
    ):
        self.chain_id = chain_id
        self.chain_config = chain_config
        self.registry_client = registry_client
        self.aggregation = aggregation
        self.pollers = []
        self.running = False

        # Health monitoring state
        self.last_health_check = 0
        self.health_check_interval = 60  # Check every minute

    def start(self):
        """Start all endpoint pollers for this chain."""
        self.running = True

        # Collect all endpoints (manual + registry)
        all_endpoints = self._collect_all_endpoints()

        logger.info(f"Starting {len(all_endpoints)} pollers for chain {self.chain_id}")

        # Create and start pollers with staggered timing
        stagger_delay = 0
        manual_endpoint_count = len(self.chain_config["endpoints"])

        for i, endpoint in enumerate(all_endpoints):
            is_manual = (
                i < manual_endpoint_count
            )  # First N are manual, rest are registry

            poller = EndpointPoller(
                url=endpoint["url"],
                chain_id=self.chain_id,
                interval=endpoint["interval"],
                aggregation=self.aggregation,
                is_manual=is_manual,
            )
            poller.start(stagger_delay)
            self.pollers.append(poller)

            # Stagger by 1 second intervals
            stagger_delay += 1

        # Start health monitoring thread
        self.health_monitor_thread = threading.Thread(
            target=self._health_monitor_loop, daemon=True
        )
        self.health_monitor_thread.start()

    def stop(self):
        """Stop all pollers for this chain."""
        self.running = False
        for poller in self.pollers:
            poller.stop()

    def _collect_all_endpoints(self) -> List[Dict]:
        """Collect all endpoints from manual config and registry."""
        endpoints = []

        # Add manual endpoints
        for endpoint in self.chain_config["endpoints"]:
            endpoints.append(endpoint)

        # Add registry endpoints if enabled
        if self.chain_config["use_chain_registry"] and self.chain_config["cname"]:
            try:
                registry_rpcs = self.registry_client.fetch_chain_rpcs(
                    self.chain_config["cname"], self.chain_id
                )

                # Limit number of registry RPCs
                max_registry = self.chain_config["max_registry_rpcs"]
                registry_rpcs = registry_rpcs[:max_registry]

                # Add registry RPCs with chain default interval
                for rpc_url in registry_rpcs:
                    endpoints.append(
                        {"url": rpc_url, "interval": self.chain_config["interval"]}
                    )

                REGISTRY_ENDPOINTS_GAUGE.labels(network=self.chain_id).set(
                    len(registry_rpcs)
                )
                logger.info(
                    f"Added {len(registry_rpcs)} registry endpoints for {self.chain_id}"
                )

            except Exception as e:
                logger.error(
                    f"Failed to fetch registry endpoints for {self.chain_id}: {e}"
                )
                REGISTRY_ENDPOINTS_GAUGE.labels(network=self.chain_id).set(0)
        else:
            REGISTRY_ENDPOINTS_GAUGE.labels(network=self.chain_id).set(0)

        return endpoints

    def _health_monitor_loop(self):
        """Monitor chain health and detect drift."""
        while self.running:
            try:
                self._check_chain_health()
                time.sleep(self.health_check_interval)
            except Exception as e:
                logger.error(f"Error in health monitor for {self.chain_id}: {e}")
                time.sleep(self.health_check_interval)

    def _check_chain_health(self):
        """Check chain health with confidence scoring and enhanced metrics."""
        healthy_count = 0
        moderate_degraded_count = 0  # 50-79% efficiency
        severe_degraded_count = 0  # <50% efficiency
        heights = []
        block_times = []
        total_efficiency = 0
        manual_healthy = 0
        registry_healthy = 0

        for poller in self.pollers:
            if poller.last_result and "error" not in poller.last_result:
                healthy_count += 1
                heights.append(poller.last_result["height"])
                if poller.last_result["block_time"] > 0:
                    block_times.append(poller.last_result["block_time"])

                # Track efficiency and source diversity
                status = poller.get_status_summary()
                efficiency = status["efficiency_pct"]
                total_efficiency += efficiency

                if poller.is_manual:
                    manual_healthy += 1
                else:
                    registry_healthy += 1

                # Categorize performance degradation
                if efficiency < 50:  # Running >2x slower than desired
                    severe_degraded_count += 1
                elif efficiency < 80:  # Running >25% slower than desired
                    moderate_degraded_count += 1

        # Calculate confidence score
        confidence = self._calculate_confidence(
            heights, block_times, healthy_count, manual_healthy, registry_healthy
        )

        # Update all metrics
        HEALTHY_ENDPOINTS_GAUGE.labels(network=self.chain_id).set(healthy_count)
        DEGRADED_ENDPOINTS_GAUGE.labels(network=self.chain_id, severity="moderate").set(
            moderate_degraded_count
        )
        DEGRADED_ENDPOINTS_GAUGE.labels(network=self.chain_id, severity="severe").set(
            severe_degraded_count
        )

        # Calculate chain-level efficiency and confidence metrics
        avg_efficiency = total_efficiency / max(1, healthy_count)
        CHAIN_EFFICIENCY_GAUGE.labels(network=self.chain_id).set(avg_efficiency)
        CHAIN_CONFIDENCE_GAUGE.labels(network=self.chain_id).set(confidence)

        # Calculate aggregated height (keeping existing logic for dashboard compatibility)
        if heights:
            if self.aggregation == "max":
                agg_height = max(heights)
            elif self.aggregation == "mean":
                agg_height = mean(heights)
            else:
                agg_height = max(heights)

            AGGREGATION_GAUGES[self.aggregation].labels(network=self.chain_id).set(
                agg_height
            )

        # Calculate height drift in time
        if len(block_times) >= 2:
            time_drift = max(block_times) - min(block_times)
            CHAIN_HEIGHT_DRIFT_GAUGE.labels(network=self.chain_id).set(time_drift)

            # Enhanced warning with confidence context
            if time_drift > 10:
                logger.warning(
                    f"Chain {self.chain_id} height drift >10s: {time_drift:.1f}s, confidence: {confidence:.1f}% "
                    f"({healthy_count} healthy endpoints). Consider adding more RPC servers."
                )

        # Enhanced health reporting with confidence
        total_endpoints = len(self.pollers)
        registry_config = self.chain_config.get("min_healthy_threshold", 3)

        if healthy_count < registry_config or confidence < 70:
            degraded_info = ""
            if moderate_degraded_count + severe_degraded_count > 0:
                degraded_info = f" ({moderate_degraded_count} moderate, {severe_degraded_count} severe degradation)"

            logger.warning(
                f"Chain {self.chain_id}: {healthy_count}/{total_endpoints} healthy endpoints, "
                f"confidence: {confidence:.1f}%, efficiency: {avg_efficiency:.1f}%{degraded_info}"
            )

        # Log adaptive status summary every 10 minutes
        if hasattr(self, "_last_status_log"):
            if time.time() - self._last_status_log > 600:  # 10 minutes
                self._log_adaptive_status_summary()
                self._last_status_log = time.time()
        else:
            self._last_status_log = time.time()

    def _calculate_confidence(
        self,
        heights: List[int],
        block_times: List[int],
        healthy_count: int,
        manual_healthy: int,
        registry_healthy: int,
    ) -> float:
        """Calculate confidence score for chain height reading (0-100)."""
        confidence = 0

        # Base confidence from healthy endpoint count
        if healthy_count >= 5:
            confidence += 40  # Strong consensus base
        elif healthy_count >= 3:
            confidence += 30  # Good consensus base
        elif healthy_count >= 2:
            confidence += 15  # Minimal consensus
        else:
            confidence += 0  # No consensus

        # Height consensus bonus (how tight the cluster is)
        if len(heights) >= 2:
            height_range = max(heights) - min(heights)
            if height_range <= 1:
                confidence += 30  # Perfect sync
            elif height_range <= 3:
                confidence += 20  # Very good sync
            elif height_range <= 10:
                confidence += 10  # Acceptable sync
            # else: no bonus for poor sync

        # Temporal freshness bonus
        if block_times:
            time_drift = max(block_times) - min(block_times)
            if time_drift <= 7:  # Within 7 seconds
                confidence += 20
            elif time_drift <= 15:  # Within 15 seconds
                confidence += 10

        # Endpoint diversity bonus (manual + registry sources)
        if manual_healthy >= 1 and registry_healthy >= 1:
            confidence += 10  # Mix of sources reduces single-point-of-failure

        return min(100.0, confidence)

    def _log_adaptive_status_summary(self):
        """Log detailed adaptive status summary for all endpoints."""
        manual_stats = {"healthy": 0, "total": 0, "efficiency": 0}
        registry_stats = {"healthy": 0, "total": 0, "efficiency": 0}

        for poller in self.pollers:
            status = poller.get_status_summary()
            stats = manual_stats if poller.is_manual else registry_stats
            stats["total"] += 1

            if status["last_success"]:
                stats["healthy"] += 1
                stats["efficiency"] += status["efficiency_pct"]

        # Calculate averages
        manual_avg_eff = manual_stats["efficiency"] / max(1, manual_stats["healthy"])
        registry_avg_eff = registry_stats["efficiency"] / max(
            1, registry_stats["healthy"]
        )

        logger.info(
            f"Chain {self.chain_id} adaptive status: "
            f"Manual({manual_stats['healthy']}/{manual_stats['total']}, {manual_avg_eff:.1f}% eff), "
            f"Registry({registry_stats['healthy']}/{registry_stats['total']}, {registry_avg_eff:.1f}% eff)"
        )


class EndpointManager:
    """Manages all chains and their endpoint pollers."""

    def __init__(self, config: dict):
        self.config = config
        self.registry_client = ChainRegistryClient(
            health_check_timeout=config["registry"]["health_check_timeout"]
        )
        self.chain_managers = {}
        self.running = False

    def start(self):
        """Start all chain managers."""
        self.running = True
        aggregation = self.config["aggregation"]

        for chain_id, chain_config in self.config["chains"].items():
            manager = ChainManager(
                chain_id, chain_config, self.registry_client, aggregation
            )
            manager.start()
            self.chain_managers[chain_id] = manager

        logger.info(f"Started endpoint manager for {len(self.chain_managers)} chains")

    def stop(self):
        """Stop all chain managers."""
        self.running = False
        for manager in self.chain_managers.values():
            manager.stop()


# Aggregation gauges (created once, reused)
AGGREGATION_GAUGES = {
    "max": Gauge(
        "blockchain_latest_block_height_max",
        "max block height across reference nodes",
        ["network"],
    ),
    "mean": Gauge(
        "blockchain_latest_block_height_mean",
        "mean block height across reference nodes",
        ["network"],
    ),
}


# Legacy functions removed - replaced by EndpointManager architecture


def run_healthz_server(port=8001):
    class HealthzHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/healthz":
                self.send_response(200)
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                self.wfile.write(b"ok")
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            return  # Silence default logging

    server = HTTPServer(("0.0.0.0", port), HealthzHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CometBFT Prometheus Height Exporter")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--healthz-port", type=int, default=8001)
    args = parser.parse_args()
    config = load_config(args.config)
    # Logging config
    log_format = config.get("log_format", "text")
    log_level = config.get("log_level", "INFO")
    setup_logging(log_format, log_level)
    logger.info(f"Loaded config: {config}")
    start_http_server(args.port)
    logger.info(f"Exporter running on :{args.port}/metrics")
    run_healthz_server(args.healthz_port)
    logger.info(f"Health endpoint running on :{args.healthz_port}/healthz")

    # Start the new endpoint manager
    manager = EndpointManager(config)
    try:
        manager.start()
        logger.info("Endpoint manager started successfully")

        # Keep main thread alive
        while True:
            time.sleep(60)  # Wake up every minute to handle KeyboardInterrupt

    except KeyboardInterrupt:
        logger.info("Shutting down...")
        manager.stop()
        logger.info("Endpoint manager stopped")


if __name__ == "__main__":
    main()
