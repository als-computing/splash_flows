#!/bin/bash
#SBATCH -q xfer
#SBATCH -A als
#SBATCH -C cron
#SBATCH --time=00:10:00
#SBATCH --job-name={job_name}
#SBATCH --output={out_pattern}
#SBATCH --error={err_pattern}

set -euo pipefail

echo "[LOG] Listing HPSS path: {full_hpss}"
{cmd}