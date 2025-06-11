# CometBFT Height Exporter

A Prometheus exporter for monitoring the latest block height and sync status of trusted reference nodes for multiple Cosmos SDK-based blockchains.

## Features
- Scrapes `/status` endpoints from trusted nodes for each configured chain
- Exposes latest block height, block time, and sync status as Prometheus metrics
- Aggregates block height across nodes (max/mean)
- Robust error handling and logging (TEXT or JSON)
- Health check endpoint for Kubernetes
- Production-ready: Docker, CI, and K8s friendly

## Configuration
Edit `config.yaml`:
```yaml
chains:
  thorchain-1:
    - https://rpc.ninerealms.com
    - https://thornode-mainnet-rpc.bryanlabs.net
  cosmoshub-4:
    - https://cosmos-rpc.polkachu.com
  osmosis-1:
    - https://osmosis-rpc.polkachu.com
  kaiyo-1:
    - https://kujira-rpc.polkachu.com
  noble-1:
    - https://noble-rpc.polkachu.com
  columbus-5:
    - https://terra-rpc.bryanlabs.net
  phoenix-1:
    - https://terra-rpc.polkachu.com
interval: 15  # seconds
aggregation: max  # or 'mean'
log_format: json  # or 'json'
log_level: INFO  # or DEBUG, WARNING, ERROR
```

## Usage
Install dependencies:
```sh
pip install -r requirements.txt
```
Run the exporter:
```sh
python exporter.py --config config.yaml --port 8000 --healthz-port 8001
```

## Endpoints
- `/metrics` (default :8000): Prometheus scrape endpoint
- `/healthz` (default :8001): Health check (returns `ok`)

## Metrics
- `blockchain_latest_block_height{node,network}`
- `blockchain_sync_catching_up{node,network}`
- `blockchain_latest_block_time{node,network}`
- `blockchain_latest_block_height_max{network}`
- `blockchain_latest_block_height_mean{network}`

## Docker
Build and run:
```sh
docker build -t cometbft-height-exporter .
docker run -p 8000:8000 -p 8001:8001 -v $(pwd)/config.yaml:/app/config.yaml cometbft-height-exporter
```

## Kubernetes
Add a liveness/readiness probe:
```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8001
  initialDelaySeconds: 5
  periodSeconds: 10
```

## CI/CD
A GitHub Actions workflow (`.github/workflows/docker.yml`) is included for linting, building, and publishing multi-arch Docker images to GitHub Container Registry (GHCR).

---

**Questions or contributions welcome!**
