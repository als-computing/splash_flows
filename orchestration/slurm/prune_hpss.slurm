#!/bin/bash
# ------------------------------------------------------------------
# SLURM Job Script for Pruning Data from HPSS
# ------------------------------------------------------------------

#SBATCH -q xfer                           # Specify the SLURM queue to use.
#SBATCH -A als                            # Specify the account.
#SBATCH -C cron                           # Use the 'cron' constraint.
#SBATCH --time=12:00:00                   # Maximum runtime of 12 hours.
#SBATCH --job-name=transfer_to_HPSS_{relative_path}  # Set a descriptive job name.
#SBATCH --output={logs_path}/{relative_path}_prune_from_hpss_%j.out       # Standard output log file.
#SBATCH --error={logs_path}/{relative_path}_prune_from_hpss_%j.err        # Standard error log file.
#SBATCH --licenses=SCRATCH                # Request the SCRATCH license.
#SBATCH --mem=20GB                        # Request #GB of memory. Default 2GB.

set -euo pipefail                        # Enable strict error checking.
echo "[LOG] Job started at: $(date)"

# Check if the file exists on HPSS
if hsi "ls {source_endpoint.full_path(relative_path)}" &> /dev/null; then
    echo "[LOG] File {relative_path} exists on HPSS. Proceeding to prune."
    # Prune the file from HPSS
    hsi "rm {source_endpoint.full_path(relative_path)}"
    echo "[LOG] File {relative_path} has been pruned from HPSS."
    hsi ls -R {source_endpoint.full_path(relative_path)}
else
    echo "[LOG] Could not find File {relative_path} does not on HPSS. Check your file path again."
    exit 0
fi
echo "[LOG] Job completed at: $(date)