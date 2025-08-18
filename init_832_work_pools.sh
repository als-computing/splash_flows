#!/usr/bin/env bash
#
# init_832_work_pools.sh
#
# Description:
#   Initializes Prefect work pools and deployments for beamline 832 using orchestration/flows/bl832/prefect.yaml as the source of truth.
#   This script:
#   - Waits for Prefect server to become ready.
#   - Creates any missing work pools defined in the prefect.yaml configuration.
#   - Deploys flows using the prefect.yaml configuration.
#
# Usage:
#   ./init_832_work_pools.sh
#
# Environment Variables:
#   PREFECT_API_URL   Override the API URL for the Prefect server.
#                     Default: http://prefect_server:4200/api
#
# Notes:
#   - Intended to be run as a one-shot init container alongside Prefect server.
#   - Idempotent: re-running will not recreate pools that already exist.
#   - Can be included in an docker-compose setup, ex:
# prefect_init:
#   build: ./splash_flows
#   container_name: prefect_init
#   command: ["/bin/bash", "/splash_flows/init_832_work_pools.sh"]
#   volumes:
#     - ./splash_flows:/splash_flows:ro
#   environment:
#     - PREFECT_API_URL=http://prefect_server:4200/api
#     - PREFECT_LOGGING_LEVEL=DEBUG
#   depends_on:
#     - prefect_server
#   networks:
#     - prenet
#   restart: "no"   # run once, then stop


set -euo pipefail

# Path to the Prefect project file
prefect_file="/splash_flows/orchestration/flows/bl832/prefect.yaml"

# If PREFECT_API_URL is already defined in the container’s environment, it will use that value.
# If not, it falls back to the default http://prefect_server:4200/api.
: "${PREFECT_API_URL:=http://prefect_server:4200/api}"

echo "[Init] Waiting for Prefect server at $PREFECT_API_URL..."

# Wait for Prefect server to be ready, querying the health endpoint
python3 - <<'EOF'
import os, time, sys
import httpx

api_url = os.environ.get("PREFECT_API_URL", "http://prefect_server:4200/api")
health_url = f"{api_url}/health"

for _ in range(60):  # try for ~3 minutes
    try:
        r = httpx.get(health_url, timeout=2.0)
        if r.status_code == 200:
            print("[Init] Prefect server is up.")
            sys.exit(0)
    except Exception:
        pass
    print("[Init] Still waiting...")
    time.sleep(3)

print("[Init] ERROR: Prefect server did not become ready in time.", file=sys.stderr)
sys.exit(1)
EOF

echo "[Init] Creating work pools defined in $prefect_file..."

python3 - <<EOF
import yaml, subprocess, sys

prefect_file = "$prefect_file"

with open(prefect_file) as f:
    config = yaml.safe_load(f)

pools = {d["work_pool"]["name"] for d in config.get("deployments", []) if "work_pool" in d}

for pool in pools:
    print(f"[Init] Ensuring pool: {pool}")
    try:
        subprocess.run(
            ["prefect", "work-pool", "inspect", pool],
            check=True,
            capture_output=True,
        )
        print(f"[Init] Work pool '{pool}' already exists.")
    except subprocess.CalledProcessError:
        print(f"[Init] Creating work pool: {pool}")
        subprocess.run(["prefect", "work-pool", "create", pool, "--type", "process"], check=True)
EOF

# Deploy flows (queues are auto-created if named)
echo "[Init] Deploying flows..."
prefect --no-prompt deploy --prefect-file "$prefect_file" --all

echo "[Init] Done."
