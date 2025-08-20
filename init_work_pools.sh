#!/usr/bin/env bash
#
# init_work_pools.sh
#
# Description:
#   Initializes Prefect work pools and deployments for the beamline defined by the BEAMLINE environment variable.
#   Uses orchestration/flows/bl"$BEAMLINE"/prefect.yaml as the single source of truth.
#
# Requirements:
#   - BEAMLINE must be set (e.g., 832).
#   - A prefect.yaml file must exist in orchestration/flows/bl"$BEAMLINE"/.
#
# Behavior:
#   - Waits until the Prefect server is reachable via its /health endpoint.
#   - Creates any missing work pools defined in the beamline's prefect.yaml.
#   - Deploys all flows defined in the beamline's prefect.yaml.
#
# Usage:
#   ./init_work_pools.sh
#
# Environment Variables:
#   BEAMLINE          The beamline identifier (e.g., 832). Required.
#   PREFECT_API_URL   Override the Prefect server API URL.
#                     Default: http://prefect_server:4200/api
#
# Notes:
#   - Intended to be run as a one-shot init container alongside a Prefect server.
#   - Idempotent: re-running will not recreate pools that already exist.
#   - Typical docker-compose integration:
#
#       prefect_init:
#         build: ./splash_flows
#         container_name: prefect_init
#         command: ["/bin/bash", "/splash_flows/init_work_pools.sh"]
#         volumes:
#           - ./splash_flows:/splash_flows:ro
#         environment:
#           - PREFECT_API_URL=http://prefect_server:4200/api
#           - PREFECT_LOGGING_LEVEL=DEBUG
#           - BEAMLINE=832
#         depends_on:
#           - prefect_server
#         networks:
#           - prenet
#         restart: "no"   # run once, then stop

set -euo pipefail

BEAMLINE="${BEAMLINE:?Must set BEAMLINE (e.g. 832, 733)}"

# Path to the Prefect project file
PREFECT_YAML="orchestration/flows/bl${BEAMLINE}/prefect.yaml"

if [[ ! -f "$PREFECT_YAML" ]]; then
  echo "[Init:${BEAMLINE}] ERROR: Expected $PREFECT_YAML not found!" >&2
  exit 1
fi

# If PREFECT_API_URL is already defined in the container’s environment, it will use that value.
# If not, it falls back to the default http://prefect_server:4200/api.
: "${PREFECT_API_URL:=http://prefect_server:4200/api}"

echo "[Init:${BEAMLINE}] Using prefect.yaml at $PREFECT_YAML"
echo "[Init:${BEAMLINE}] Waiting for Prefect server at $PREFECT_API_URL..."

# Wait for Prefect server to be ready, querying the health endpoint
python3 - <<EOF
import os, time, sys
import httpx
beamline = "$BEAMLINE"

api_url = os.environ.get("PREFECT_API_URL", "http://prefect_server:4200/api")
if "api.prefect.cloud" in api_url:
    print(f"[Init:{beamline}] Prefect Cloud detected — skipping health check.")
    sys.exit(0)
else:
    health_url = f"{api_url}/health"
    print(f"[Init:{beamline}] Ping self-hosted health endpoint: {health_url}")

for _ in range(60):  # try for ~3 minutes
    try:
        r = httpx.get(health_url, timeout=2.0)
        if r.status_code == 200:
            print(f"[Init:{beamline}] Prefect server is up.")
            sys.exit(0)
    except Exception:
        pass
    print(f"[Init:{beamline}] Still waiting...")
    time.sleep(3)

print(f"[Init:{beamline}] ERROR: Prefect server did not become ready in time.", file=sys.stderr)
sys.exit(1)
EOF

echo "[Init:${BEAMLINE}] Creating work pools from $PREFECT_YAML..."

python3 - <<EOF
import yaml, subprocess, sys

prefect_file = "$PREFECT_YAML"
beamline = "$BEAMLINE"

with open(prefect_file) as f:
    config = yaml.safe_load(f)

pools = {d["work_pool"]["name"] for d in config.get("deployments", []) if "work_pool" in d}

for pool in pools:
    print(f"[Init:{beamline}] Ensuring pool: {pool}")
    try:
        subprocess.run(
            ["prefect", "work-pool", "inspect", pool],
            check=True,
            capture_output=True,
        )
        print(f"[Init:{beamline}] Work pool '{pool}' already exists.")
    except subprocess.CalledProcessError:
        print(f"[Init:{beamline}] Creating work pool: {pool}")
        subprocess.run(["prefect", "work-pool", "create", pool, "--type", "process"], check=True)
EOF

# Deploy flows (queues are auto-created if named)
echo "[Init:${BEAMLINE}] Deploying flows..."
prefect --no-prompt deploy --prefect-file "$PREFECT_YAML" --all
echo "[Init:${BEAMLINE}] Done."
