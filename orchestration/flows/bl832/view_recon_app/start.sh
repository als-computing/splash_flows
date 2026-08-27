#!/bin/bash

# Activate the conda environment
source activate tiled_zarr_env

# Set ports from environment variables or use defaults
TILED_PORT=${TILED_PORT:-8000}
VIEWER_PORT=${VIEWER_PORT:-8082}
REACT_PORT=${REACT_PORT:-5174}

# Define data directory with a default that can be overridden
DATA_DIR=${DATA_DIR:-"/data"}

# Ensure data directory exists
mkdir -p "${DATA_DIR}"
echo "Data directory is set to: ${DATA_DIR}"
ls -la "${DATA_DIR}"

# Set CORS origins for local development and container use
export TILED_ALLOW_ORIGINS="http://localhost:3000 http://localhost:${REACT_PORT} http://localhost:${VIEWER_PORT} http://127.0.0.1:${REACT_PORT} http://127.0.0.1:${VIEWER_PORT}"

# Start Tiled server in the background (bind to all interfaces)
echo "Starting Tiled server with data from ${DATA_DIR} on port ${TILED_PORT}..."
tiled serve directory "${DATA_DIR}" --public --watch True --verbose --host 0.0.0.0 --port ${TILED_PORT} &
TILED_PID=$!

# Wait for Tiled to start
echo "Waiting for Tiled server to start..."
sleep 5

export HOST=0.0.0.0

# Start itk-vtk-viewer in the background
echo "Starting itk-vtk-viewer on port ${VIEWER_PORT}..."
itk-vtk-viewer --port ${VIEWER_PORT} &
VIEWER_PID=$!

# Wait for itk-vtk-viewer to start
echo "Waiting for itk-vtk-viewer to start..."
sleep 5

# Start the React app
echo "Starting React app on port ${REACT_PORT}..."
cd /app/splash_flows_globus/orchestration/flows/bl832/view_recon_app
npm run dev -- --host 0.0.0.0 --port ${REACT_PORT} &
REACT_PID=$!

# Function to handle shutdown
function cleanup {
  echo "Shutting down services..."
  kill $REACT_PID
  kill $VIEWER_PID
  kill $TILED_PID
  exit 0
}

# Register the cleanup function for signals
trap cleanup SIGINT SIGTERM

# Keep the script running and monitor services
echo "All services started."
echo "- React app: http://localhost:${REACT_PORT}"
echo "- Tiled server: http://localhost:${TILED_PORT}"
echo "- itk-vtk-viewer: http://localhost:${VIEWER_PORT}"
echo "Press Ctrl+C to stop all services."

# Keep checking if services are still running
while true; do
  if ! ps -p $TILED_PID > /dev/null; then
    echo "Tiled server has stopped unexpectedly. Restarting..."
    tiled serve directory "${DATA_DIR}" --public --verbose --host 0.0.0.0 --port ${TILED_PORT} &
    TILED_PID=$!
  fi
  
  if ! ps -p $VIEWER_PID > /dev/null; then
    echo "itk-vtk-viewer has stopped unexpectedly. Restarting..."
    itk-vtk-viewer --port ${VIEWER_PORT} &
    VIEWER_PID=$!
  fi
  
  if ! ps -p $REACT_PID > /dev/null; then
    echo "React app has stopped unexpectedly. Restarting..."
    cd /app/splash_flows_globus/orchestration/flows/bl832/view_recon_app
    npm run dev -- --host 0.0.0.0 --port ${REACT_PORT} &
    REACT_PID=$!
  fi
  
  sleep 10
done
