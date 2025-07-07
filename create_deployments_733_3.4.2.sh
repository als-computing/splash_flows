#!/usr/bin/env bash

# Create deployments for the 733 flows in the Prefect 3.4.2 version

set -euo pipefail

# Load environment variables (e.g., PREFECT_API_URL, PREFECT_API_KEY)
export $(grep -v '^#' .env | xargs)

# Set working directory to the flows folder
cd orchestration/flows/bl733/

# Create required work pools (warns but doesn't fail if they already exist)
prefect work-pool create new_file_733_pool || true
prefect work-pool create dispatcher_733_pool || true
prefect work-pool create prune_733_pool || true

# Register deployments from the prefect.yaml config
prefect deploy
