#!/usr/bin/env bash
set -euo pipefail
: "${PREFECT_API_URL:=http://localhost:4200/api}"

mk_pool () {
  local pool="$1"
  prefect work-pool inspect "$pool" >/dev/null 2>&1 || \
    prefect work-pool create "$pool" --type process
}

mk_pool dispatcher_832_pool
mk_pool new_file_832_pool
mk_pool prune_832_pool
mk_pool alcf_recon_flow_pool
mk_pool nersc_recon_flow_pool
mk_pool nersc_streaming_pool

# Create deployments; queues are optional and will be auto-created if named
prefect --no-prompt deploy --prefect-file orchestration/flows/bl832/prefect.yaml --all