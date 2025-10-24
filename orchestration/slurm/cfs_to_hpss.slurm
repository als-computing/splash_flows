#!/bin/bash
# ------------------------------------------------------------------
# SLURM Job Script for Transferring Data from CFS to HPSS
# This script will:
#   1. Define the source (CFS) and destination (HPSS) paths.
#   2. Create the destination directory on HPSS if it doesn't exist.
#   3. Determine if the source is a file or a directory.
#      - If a file, transfer it using 'hsi cput'.
#      - If a directory, group files by beam cycle and archive them.
#         * Cycle 1: Jan 1 - Jul 15
#         * Cycle 2: Jul 16 - Dec 31
#         * If a group exceeds 2 TB, it is partitioned into multiple tar archives.
#         * Archive names:
#              [proposal_name]_[year]-[cycle].tar
#              [proposal_name]_[year]-[cycle]_part0.tar, _part1.tar, etc.
#   4. Echo directory trees for both source and destination for logging.
# ------------------------------------------------------------------

#SBATCH -q xfer                           # Specify the SLURM queue to use.
#SBATCH -A als                            # Specify the account.
#SBATCH -C cron                           # Use the 'cron' constraint.
#SBATCH --time=12:00:00                   # Maximum runtime of 12 hours.
#SBATCH --job-name=transfer_to_HPSS_{proposal_name}  # Set a descriptive job name.
#SBATCH --output={logs_path}/{proposal_name}_to_hpss_%j.out       # Standard output log file.
#SBATCH --error={logs_path}/{proposal_name}_to_hpss_%j.err        # Standard error log file.
#SBATCH --licenses=SCRATCH                # Request the SCRATCH license.
#SBATCH --mem=2GB                        # Request #GB of memory. Default 2GB.

set -euo pipefail                        # Enable strict error checking.
echo "[LOG] Job started at: $(date)"

# ------------------------------------------------------------------
# Define source and destination variables.
# ------------------------------------------------------------------

echo "[LOG] Defining source and destination paths."

# SOURCE_PATH: Full path of the file or directory on CFS.
SOURCE_PATH="{full_cfs_path}"
echo "[LOG] SOURCE_PATH set to: $SOURCE_PATH"

# DEST_ROOT: Root destination on HPSS built from configuration.
DEST_ROOT="{hpss_root_path}"
echo "[LOG] DEST_ROOT set to: $DEST_ROOT"

# FOLDER_NAME: Proposal name (project folder) derived from the file path.
FOLDER_NAME="{proposal_name}"
echo "[LOG] FOLDER_NAME set to: $FOLDER_NAME"

# DEST_PATH: Final HPSS destination directory.
DEST_PATH="${{DEST_ROOT}}/${{FOLDER_NAME}}"
echo "[LOG] DEST_PATH set to: $DEST_PATH"

# ------------------------------------------------------------------
# Create destination directory on HPSS recursively using hsi mkdir.
# This section ensures that the entire directory tree specified in DEST_PATH
# exists on HPSS. Since HPSS hsi does not support a recursive mkdir option,
# we split the path into its components and create each directory one by one.
# ------------------------------------------------------------------

echo "[LOG] Checking if HPSS destination directory exists at $DEST_PATH."

# Use 'hsi ls' to verify if the destination directory exists.
# The '-q' flag is used for quiet mode, and any output or errors are discarded.
if hsi -q "ls $DEST_PATH" >/dev/null 2>&1; then
    echo "[LOG] Destination directory $DEST_PATH already exists."
else
    # If the directory does not exist, begin the process to create it.
    echo "[LOG] Destination directory $DEST_PATH does not exist. Attempting to create it recursively."

    # Initialize an empty variable 'current' that will store the path built so far.
    current=""

    # Split the DEST_PATH using '/' as the delimiter.
    # This creates an array 'parts' where each element is a directory level in the path.
    IFS='/' read -ra parts <<< "$DEST_PATH"

    # Iterate over each directory component in the 'parts' array.
    for part in "${{parts[@]}}"; do
        # Skip any empty parts. An empty string may occur if the path starts with a '/'.
        if [ -z "$part" ]; then
            continue
        fi

        # Append the current part to the 'current' path variable.
        # This step incrementally reconstructs the full path one directory at a time.
        current="$current/$part"

        # Check if the current directory exists on HPSS using 'hsi ls'.
        if ! hsi -q "ls $current" >/dev/null 2>&1; then
            # If the directory does not exist, attempt to create it using 'hsi mkdir'.
            if hsi "mkdir $current" >/dev/null 2>&1; then
                echo "[LOG] Created directory $current."
            else
                echo "[ERROR] Failed to create directory $current."
                exit 1
            fi
        else
            echo "[LOG] Directory $current already exists."
        fi
    done
fi

# List the final HPSS directory tree for logging purposes.
# For some reason this gets logged in the project.err file, not the .out file.
hsi ls $DEST_PATH

# ------------------------------------------------------------------
# Transfer Logic: Check if SOURCE_PATH is a file or directory.
# ------------------------------------------------------------------

echo "[LOG] Determining type of SOURCE_PATH: $SOURCE_PATH"
if [ -f "$SOURCE_PATH" ]; then
    # Case: Single file detected.
    echo "[LOG] Single file detected. Transferring via hsi cput."
    FILE_NAME=$(basename "$SOURCE_PATH")
    echo "[LOG] File name: $FILE_NAME"
    # Note about hsi cput: If the file already exists on HPSS, hsi cput will skip the transfer.
    hsi cput "$SOURCE_PATH" "$DEST_PATH/$FILE_NAME"
    echo "[LOG] (Simulated) File transfer completed for $FILE_NAME."
elif [ -d "$SOURCE_PATH" ]; then
    # Case: Directory detected.
    echo "[LOG] Directory detected. Initiating bundling process."

    # ------------------------------------------------------------------
    # Define thresholds
    #    - THRESHOLD: maximum total size per HTAR archive (2 TB).
    #    - MEMBER_LIMIT: maximum size per member file in an HTAR (set to 65 GB).
    # ------------------------------------------------------------------

    THRESHOLD=2199023255552  # 2 TB in bytes.
    MEMBER_LIMIT=$((65*1024**3))  # 65 GB in bytes. 68 GB is the htar limit. Move files >65 GB than this using hsi cput.
    echo "[LOG] Threshold set to 2 TB (bytes): $THRESHOLD"
    echo "[LOG] Threshold for individual file transfer (bytes): $MEMBER_LIMIT"

    # ------------------------------------------------------------------
    # Generate a list of relative file paths in the project directory.
    # This list will be used to group files by their modification date.
    # ------------------------------------------------------------------
    # Create a temporary file to store the list of relative file paths.
    # Explanation:
    # 1. FILE_LIST=$(mktemp)
    #    - mktemp creates a unique temporary file and its path is stored in FILE_LIST.
    #
    # 2. (cd "$SOURCE_PATH" && find . -type f | sed 's|^\./||')
    #    - The parentheses run the commands in a subshell, so the directory change does not affect the current shell.
    #    - cd "$SOURCE_PATH": Changes the working directory to the source directory.
    #    - find . -type f: Recursively finds all files starting from the current directory (which is now SOURCE_PATH),
    #      outputting paths prefixed with "./".
    #    - sed 's|^\./||': Removes the leading "./" from each file path, resulting in relative paths without the prefix.
    #
    # 3. The output is then redirected into the temporary file specified by FILE_LIST.
    # ------------------------------------------------------------------

    echo "[LOG] Grouping files by modification date."

    FILE_LIST=$(mktemp)
    (cd "$SOURCE_PATH" && find . -type f | sed 's|^\./||') > "$FILE_LIST"

    echo "[LOG] List of files stored in temporary file: $FILE_LIST"

    # ------------------------------------------------------------------
    # Filter out oversized files (>65GB) for immediate transfer
    #    - For each file:
    #        • If fsize > MEMBER_LIMIT: transfer via hsi cput.
    #        • Else: add path to new list for bundling.
    # ------------------------------------------------------------------

    echo "[LOG] Beginning oversized-file filtering (> $MEMBER_LIMIT bytes)"
    FILTERED_LIST=$(mktemp)
    echo "[LOG] Writing remaining file paths to $FILTERED_LIST"

    while IFS= read -r f; do
        # Absolute local path and size
        full_local="$SOURCE_PATH/$f"
        fsize=$(stat -c %s "$full_local")

        if (( fsize > MEMBER_LIMIT )); then
            # Relative subdirectory and filename
            rel_dir=$(dirname "$f")
            fname=$(basename "$f")

            # Compute HPSS directory under project (create if needed)
            if [ "$rel_dir" = "." ]; then
                dest_dir="$DEST_PATH"
            else
                dest_dir="$DEST_PATH/$rel_dir"
            fi

            if ! hsi -q "ls $dest_dir" >/dev/null 2>&1; then
                echo "[LOG] Creating HPSS directory $dest_dir"
                hsi mkdir "$dest_dir"
            fi

            # Full remote file path (directory + filename)
            remote_file="$dest_dir/$fname"

            # Transfer via conditional put
            echo "[LOG] Transferring oversized file '$f' ($fsize bytes) to HPSS path $remote_file"
            echo "[DEBUG] hsi cput \"$full_local\" : \"$remote_file\""
            hsi cput "$full_local" : "$remote_file"
            echo "[LOG] Completed hsi cput for '$f'."
        else
            # Keep for bundling later
            echo "$f" >> "$FILTERED_LIST"
        fi
    done < "$FILE_LIST"

    # Swap in the filtered list and report
    mv "$FILTERED_LIST" "$FILE_LIST"
    remaining=$(wc -l < "$FILE_LIST")
    echo "[LOG] Oversized-file transfer done. Remaining for bundling: $remaining files."

    # ------------------------------------------------------------------
    # Cycle-based grouping & tar-bundling logic (unchanged).
    # ------------------------------------------------------------------

    # Declare associative arrays to hold grouped file paths and sizes.
    declare -A group_files
    declare -A group_sizes

    # ------------------------------------------------------------------
    # Group files by modification date.
    # ------------------------------------------------------------------

    cd "$SOURCE_PATH" && \
    while IFS= read -r file; do
        mtime=$(stat -c %Y "$file")
        year=$(date -d @"$mtime" +%Y)
        month=$(date -d @"$mtime" +%m | sed 's/^0*//')
        day=$(date -d @"$mtime" +%d | sed 's/^0*//')
        # Determine cycle: Cycle 1 if month < 7 or (month == 7 and day <= 15), else Cycle 2.
        if [ "$month" -lt 7 ] || {{ [ "$month" -eq 7 ] && [ "$day" -le 15 ]; }}; then
            cycle=1
        else
            cycle=2
        fi
        key="${{year}}-${{cycle}}"
        group_files["$key"]="${{group_files["$key"]:-}} $file"
        fsize=$(stat -c %s "$file")
        group_sizes["$key"]=$(( ${{group_sizes["$key"]:-0}} + fsize ))
    done < "$FILE_LIST"
    rm "$FILE_LIST"
    echo "[LOG] Completed grouping files."

    # Print the files in each group at the end
    for key in "${{!group_files[@]}}"; do
        echo "[LOG] Group $key contains files:"
        for f in ${{group_files["$key"]}}; do
            echo "    $f"
        done
    done

    # ------------------------------------------------------------------
    # Bundle files into tar archives.
    # ------------------------------------------------------------------
    for key in "${{!group_files[@]}}"; do
        files=(${{group_files["$key"]}})
        total_group_size=${{group_sizes["$key"]}}
        echo "[LOG] Processing group $key with ${{#files[@]}} files; total size: $total_group_size bytes."

        part=0
        current_size=0
        current_files=()
        for f in "${{files[@]}}"; do
            fsize=$(stat -c %s "$f")
            # If adding this file exceeds the threshold, process the current bundle.
            if (( current_size + fsize > THRESHOLD && ${{#current_files[@]}} > 0 )); then
                if [ $part -eq 0 ]; then
                    tar_name="${{FOLDER_NAME}}_${{key}}.tar"
                else
                    tar_name="${{FOLDER_NAME}}_${{key}}_part${{part}}.tar"
                fi
                echo "[LOG] Bundle reached threshold."
                echo "[LOG] Files in current bundle:"
                for file in "${{current_files[@]}}"; do
                    echo "$file"
                done
                echo "[LOG] Creating archive $tar_name with ${{#current_files[@]}} files; bundle size: $current_size bytes."
                (cd "$SOURCE_PATH" && htar -cvf "${{DEST_PATH}}/${{tar_name}}" $(printf "%s " "${{current_files[@]}}"))
                part=$((part+1))
                echo "[DEBUG] Resetting bundle variables."
                current_files=()
                current_size=0
            fi
            current_files+=("$f")
            current_size=$(( current_size + fsize ))
        done
        if [ ${{#current_files[@]}} -gt 0 ]; then
            if [ $part -eq 0 ]; then
                tar_name="${{FOLDER_NAME}}_${{key}}.tar"
            else
                tar_name="${{FOLDER_NAME}}_${{key}}_part${{part}}.tar"
            fi
            echo "[LOG] Final bundle for group $key:"
            echo "[LOG] Files in final bundle:"
            for file in "${{current_files[@]}}"; do
                echo "$file"
            done
            echo "[LOG] Creating final archive $tar_name with ${{#current_files[@]}} files."
            echo "[LOG] Bundle size: $current_size bytes."
            (cd "$SOURCE_PATH" && htar -cvf "${{DEST_PATH}}/${{tar_name}}" $(printf "%s " "${{current_files[@]}}"))
        fi
        echo "[LOG] Completed processing group $key."
    done
else
    echo "[ERROR] $SOURCE_PATH is neither a file nor a directory. Exiting."
    exit 1
fi

# ------------------------------------------------------------------
# Logging: Display directory trees for both source and destination.
# ------------------------------------------------------------------
echo "[LOG] Listing Source (CFS) Tree:"
if [ -d "$SOURCE_PATH" ]; then
    find "$SOURCE_PATH" -print
else
    echo "[LOG] $SOURCE_PATH is a file."
fi

echo "[LOG] Listing Destination (HPSS) Tree:"
hsi ls -R "$DEST_PATH" || echo "[ERROR] Failed to list HPSS tree at $DEST_PATH"

echo "[LOG] Job completed at: $(date)"