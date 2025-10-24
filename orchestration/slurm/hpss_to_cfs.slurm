#!/bin/bash
#SBATCH -q xfer                                             # Specify the SLURM queue
#SBATCH -A als                                              # Specify the account.
#SBATCH -C cron                                             # Use the 'cron' constraint.
#SBATCH --time=12:00:00                                     # Maximum runtime of 12 hours.
#SBATCH --job-name=transfer_from_HPSS_{file_path}           # Set a descriptive job name.
#SBATCH --output={logs_path}/{sanitized_path}_from_hpss_%j.out   # Standard output log file.
#SBATCH --error={logs_path}/{sanitized_path}_from_hpss_%j.err    # Standard error log file.
#SBATCH --licenses=SCRATCH                                  # Request the SCRATCH license.
#SBATCH --mem=2GB                                          # Request #GB of memory. Default 2GB.
set -euo pipefail                                           # Enable strict error checking.
echo "[LOG] Job started at: $(date)"

# -------------------------------------------------------------------
# Define source and destination variables.
# -------------------------------------------------------------------

echo "[LOG] Defining source and destination paths."

# SOURCE_PATH: Full path of the file or directory on HPSS.
SOURCE_PATH="{hpss_path}"
echo "[LOG] SOURCE_PATH set to: $SOURCE_PATH"

# DEST_ROOT: Root destination on CFS built from configuration.
DEST_ROOT="{dest_root}"
echo "[LOG] DEST_ROOT set to: $DEST_ROOT"

# FILES_TO_EXTRACT: Specific files to extract from the tar archive, if any.
# If not provided, this will be empty.
FILES_TO_EXTRACT="{files_to_extract_str}"
echo "[LOG] FILES_TO_EXTRACT set to: $FILES_TO_EXTRACT"

# -------------------------------------------------------------------
# Verify that SOURCE_PATH exists on HPSS using hsi ls.
# -------------------------------------------------------------------

echo "[LOG] Verifying file existence with hsi ls."
if ! hsi ls "$SOURCE_PATH" >/dev/null 2>&1; then
    echo "[ERROR] File not found on HPSS: $SOURCE_PATH"
    exit 1
fi

# -------------------------------------------------------------------
# Determine the transfer mode based on the type (file vs tar).
# -------------------------------------------------------------------

echo "[LOG] Determining transfer mode based on the type (file vs tar)."

# Check if SOURCE_PATH ends with .tar
if [[ "$SOURCE_PATH" =~ \.tar$ ]]; then
    # If FILES_TO_EXTRACT is nonempty, MODE becomes "partial", else MODE is "tar".
    if [ -n "${{FILES_TO_EXTRACT}}" ]; then
         MODE="partial"
    else
         MODE="tar"
    fi
else
    MODE="single"
fi

echo "Transfer mode: $MODE"

# -------------------------------------------------------------------
# Transfer Logic: Based on the mode, perform the appropriate transfer.
# -------------------------------------------------------------------

if [ "$MODE" = "single" ]; then
    echo "[LOG] Single file detected. Using hsi get."
    mkdir -p "$DEST_ROOT"
    hsi get "$SOURCE_PATH" "$DEST_ROOT/"
elif [ "$MODE" = "tar" ]; then
    echo "[LOG] Tar archive detected. Extracting entire archive using htar."
    ARCHIVE_BASENAME=$(basename "$SOURCE_PATH")
    ARCHIVE_NAME="${{ARCHIVE_BASENAME%.tar}}"
    DEST_PATH="${{DEST_ROOT}}/${{ARCHIVE_NAME}}"
    echo "[LOG] Extracting to: $DEST_PATH"
    mkdir -p "$DEST_PATH"
    htar -xvf "$SOURCE_PATH" -C "$DEST_PATH"
elif [ "$MODE" = "partial" ]; then
    echo "[LOG] Partial extraction detected. Extracting selected files using htar."
    ARCHIVE_BASENAME=$(basename "$SOURCE_PATH")
    ARCHIVE_NAME="${{ARCHIVE_BASENAME%.tar}}"
    DEST_PATH="${{DEST_ROOT}}/${{ARCHIVE_NAME}}"

    # Verify that each requested file exists in the tar archive.
    echo "[LOG] Verifying requested files are in the tar archive."
    ARCHIVE_CONTENTS=$(htar -tvf "$SOURCE_PATH")
    echo "[LOG] List: $ARCHIVE_CONTENTS"
    for file in $FILES_TO_EXTRACT; do
        echo "[LOG] Checking for file: $file"
        if ! echo "$ARCHIVE_CONTENTS" | grep -q "$file"; then
            echo "[ERROR] Requested file '$file' not found in archive $SOURCE_PATH"
            exit 1
        else
            echo "[LOG] File '$file' found in archive."
        fi
    done

    echo "[LOG] All requested files verified. Proceeding with extraction."
    mkdir -p "$DEST_PATH"
    (cd "$DEST_PATH" && htar -xvf "$SOURCE_PATH" -Hnostage $FILES_TO_EXTRACT)

    echo "[LOG] Extraction complete. Listing contents of $DEST_PATH:"
    ls -l "$DEST_PATH"

else
    echo "[ERROR]: Unknown mode: $MODE"
    exit 1
fi

date