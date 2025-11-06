#!/usr/bin/env python3
"""
init_work_pools.py

Description:
    Initializes Prefect work pools and deployments for the beamline defined by the BEAMLINE environment variable.
    Uses orchestration/flows/bl"$BEAMLINE"/prefect.yaml as the single source of truth.

Requirements:
    - BEAMLINE must be set (e.g., 832).
    - A prefect.yaml file must exist in orchestration/flows/bl"$BEAMLINE"/.
    - Prefect CLI must be installed and available in PATH.

Behavior:
    - Waits until the Prefect server is reachable via its /health endpoint.
    - Creates any missing work pools defined in the beamline's prefect.yaml.
    - Deploys all flows defined in the beamline's prefect.yaml.
    - Creates/updates Prefect Secret blocks for GLOBUS_CLIENT_ID and GLOBUS_CLIENT_SECRET
      if the corresponding environment variables are present. Otherwise warns and continues.


Environment Variables:
    BEAMLINE          The beamline identifier (e.g., 832). Required.
    PREFECT_API_URL   Override the Prefect server API URL.
                      Default: http://prefect_server:4200/api
"""

import httpx
import logging
import os
import subprocess
import sys
import time
import yaml

from prefect.blocks.system import Secret


# ---------------- Logging Setup ---------------- #
logger = logging.getLogger("init_work_pools")
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    fmt="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
# ------------------------------------------------ #


def check_env() -> tuple[str, str, str]:
    """Validate required environment variables and paths."""
    beamline = os.environ.get("BEAMLINE")
    if not beamline:
        logger.error("Must set BEAMLINE (e.g., 832, 733)")
        sys.exit(1)

    prefect_yaml = f"orchestration/flows/bl{beamline}/prefect.yaml"
    if not os.path.isfile(prefect_yaml):
        logger.error(f"[Init:{beamline}] Expected {prefect_yaml} not found!")
        sys.exit(1)

    api_url = os.environ.get("PREFECT_API_URL", "http://prefect_server:4200/api")
    return beamline, prefect_yaml, api_url


def wait_for_prefect_server(api_url: str, beamline: str, timeout: int = 180):
    """Wait until Prefect server health endpoint responds (unless using Prefect Cloud)."""
    if "api.prefect.cloud" in api_url:
        logger.info(f"[Init:{beamline}] Prefect Cloud detected — skipping health check.")
        return

    health_url = f"{api_url}/health"
    logger.info(f"[Init:{beamline}] Waiting for Prefect server at {health_url}...")

    start = time.time()
    while time.time() - start < timeout:
        try:
            r = httpx.get(health_url, timeout=2.0)
            if r.status_code == 200:
                logger.info(f"[Init:{beamline}] Prefect server is up.")
                return
        except Exception:
            pass
        logger.warning(f"[Init:{beamline}] Still waiting...")
        time.sleep(3)

    logger.error(f"[Init:{beamline}] Prefect server did not become ready in time.")
    sys.exit(1)


def ensure_work_pools(prefect_yaml: str, beamline: str):
    """Ensure that all work pools from prefect.yaml exist (create if missing)."""
    with open(prefect_yaml, "r") as f:
        config = yaml.safe_load(f)

    pools = {d["work_pool"]["name"] for d in config.get("deployments", []) if "work_pool" in d}

    for pool in pools:
        logger.info(f"[Init:{beamline}] Ensuring pool: {pool}")
        try:
            subprocess.run(
                ["prefect", "work-pool", "inspect", pool],
                check=True,
                capture_output=True,
            )
            logger.info(f"[Init:{beamline}] Work pool '{pool}' already exists.")
        except subprocess.CalledProcessError:
            logger.info(f"[Init:{beamline}] Creating work pool: {pool}")
            subprocess.run(
                ["prefect", "work-pool", "create", pool, "--type", "process"],
                check=True,
            )


def deploy_flows(prefect_yaml: str, beamline: str):
    """Deploy flows defined in prefect.yaml using Prefect CLI."""
    logger.info(f"[Init:{beamline}] Deploying flows from {prefect_yaml}...")
    subprocess.run(
        ["prefect", "--no-prompt", "deploy", "--prefect-file", prefect_yaml, "--all"],
        check=True,
    )
    logger.info(f"[Init:{beamline}] Done.")


def ensure_globus_secrets(beamline: str):
    globus_client_id = os.environ.get("GLOBUS_CLIENT_ID")
    globus_client_secret = os.environ.get("GLOBUS_CLIENT_SECRET")

    if globus_client_id and globus_client_secret:
        # Create or update Prefect Secret blocks for Globus credentials
        try:
            Secret(value=globus_client_id).save(name="globus-client-id", overwrite=True)
            Secret(value=globus_client_secret).save(name="globus-client-secret", overwrite=True)
            logger.info(f"[Init:{beamline}] Created/updated Prefect Secret blocks for Globus credentials.")
        except Exception as e:
            logger.warning(f"[Init:{beamline}] Failed to create/update Prefect Secret blocks: {str(e)}")


def main():
    beamline, prefect_yaml, api_url = check_env()
    logger.info(f"[Init:{beamline}] Using prefect.yaml at {prefect_yaml}")
    wait_for_prefect_server(api_url, beamline)
    ensure_globus_secrets(beamline)
    ensure_work_pools(prefect_yaml, beamline)
    deploy_flows(prefect_yaml, beamline)


if __name__ == "__main__":
    main()
