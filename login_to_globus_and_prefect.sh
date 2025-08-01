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

export PREFECT_API_KEY="$PREFECT_API_KEY"
export PREFECT_API_URL="$PREFECT_API_URL"