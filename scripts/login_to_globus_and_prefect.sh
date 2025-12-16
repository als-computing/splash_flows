#!/bin/bash
# Run this script to login to Globus and Prefect from env variables
# Example: source ./login_to_globus_and_prefect.sh
# Load environment variables from the .env file
if [ -f .env ]; then
  source .env

else
  echo ".env file not found"
  exit 1
fi

export GLOBUS_CLIENT_ID="$GLOBUS_CLIENT_ID"
export GLOBUS_CLIENT_SECRET="$GLOBUS_CLIENT_SECRET"
export GLOBUS_CLI_CLIENT_ID="$GLOBUS_CLIENT_ID"
export GLOBUS_CLI_CLIENT_SECRET="$GLOBUS_CLIENT_SECRET"
export GLOBUS_COMPUTE_CLIENT_ID="$GLOBUS_CLIENT_ID"
export GLOBUS_COMPUTE_CLIENT_SECRET="$GLOBUS_CLIENT_SECRET"
export PREFECT_API_KEY="$PREFECT_API_KEY"
export PREFECT_API_URL="$PREFECT_API_URL"
export SCICAT_API_URL="$SCICAT_API_URL"
export SCICAT_INGEST_USER="$SCICAT_INGEST_USER"
export SCICAT_INGEST_PASSWORD="$SCICAT_INGEST_PASSWORD"