#!/bin/bash
# Create BL7012 deployment
# Works with both local Prefect server and remote deployments

set -e

echo "Creating BL7012 deployment..."
echo ""

# Load environment variables from .env
if [ ! -f .env ]; then
    echo "Error: .env file not found"
    echo "Please create a .env file with required variables"
    exit 1
fi

export $(grep -v '^#' .env | xargs) 2>/dev/null || true
echo "✓ Loaded .env"

# Use PREFECT_API_URL from .env if set, otherwise default to local
PREFECT_API_URL=${PREFECT_API_URL:-http://127.0.0.1:4200/api}
export PREFECT_API_URL

# Use PREFECT_API_KEY from .env if set (not needed for local server)
if [ -n "$PREFECT_API_KEY" ]; then
    export PREFECT_API_KEY
    echo "✓ Using API key for authentication"
fi

echo "Target: $PREFECT_API_URL"
echo ""

# Check if server is running
if ! curl -s -f "$PREFECT_API_URL/health" > /dev/null 2>&1; then
    echo "Error: Cannot connect to Prefect server at $PREFECT_API_URL"
    if [ "$PREFECT_API_URL" = "http://127.0.0.1:4200/api" ]; then
        echo "Start local server with: prefect server start"
    else
        echo "Check that the remote server is accessible"
    fi
    exit 1
fi

echo "✓ Server is running"
echo ""

# Deploy using modern command
# Work pool: process_newdata7012_ptycho4 (production)
# Work queue: bl7012_ptycho4
prefect deploy \
    --name bl7012-process-new-file \
    --pool process_newdata7012_ptycho4 \
    --work-queue bl7012_ptycho4 \
    orchestration/flows/bl7012/move.py:process_new_file

echo ""
echo "✓ Deployment created!"
echo ""
echo "Deployment details:"
echo "  Name: bl7012-process-new-file"
echo "  Flow: process_newfile_7012_ptycho4"
echo "  Work Pool: process_newdata7012_ptycho4"
echo "  Work Queue: bl7012_ptycho4"
echo ""

# Old version (deprecated commands - kept for reference):
# prefect deployment build ./orchestration/flows/bl7012/move.py:process_new_file_ptycho4 -n 'process_newdata7012_ptycho4' -q bl7012_ptycho4
# prefect deployment apply process_new_file_ptycho4-deployment.yaml
#
# Note: The flow function is 'process_new_file' but the @flow decorator names it 'process_newfile_7012_ptycho4'
# The function name is what matters for 'prefect deploy'

