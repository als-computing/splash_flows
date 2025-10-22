FROM continuumio/miniconda3:25.1.1-2

# Install system dependencies
RUN apt-get update && \
    apt-get install -y git curl build-essential socat && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Node.js 20.x (LTS)
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Verify Node.js and npm versions
RUN node --version && npm --version

# Skip Chromium download for puppeteer
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
ENV PUPPETEER_SKIP_DOWNLOAD=true

# Clone the custom itk-vtk-viewer repo but modify the package.json to skip puppeteer
WORKDIR /tmp
RUN git clone -b iframe https://github.com/als-computing/itk-vtk-viewer.git && \
    cd itk-vtk-viewer && \
    # Remove puppeteer from package.json
    sed -i 's/"puppeteer": "^5.5.0",//' package.json && \
    # Install dependencies without puppeteer
    npm install && \
    # Build and link
    npm run build && \
    npm link

# Set up conda environment for Tiled with Zarr support
RUN conda create -y -n tiled_zarr_env python=3.12 && \
    echo "source activate tiled_zarr_env" > ~/.bashrc
ENV PATH /opt/conda/envs/tiled_zarr_env/bin:$PATH

RUN pip install --upgrade starlette==0.41.2

# Clone and install the Tiled fork with Zarr support
WORKDIR /app
RUN git clone -b add-zarr-forked https://github.com/davramov/tiled.git && \
    cd tiled && \
    pip install -e ".[all]"

RUN pip install zarr==2.18.3

# Clone the React app
WORKDIR /app
RUN git clone https://github.com/davramov/splash_flows_globus.git -b issue_47 && \
    cd splash_flows_globus/orchestration/flows/bl832/view_recon_app && \
    npm install bluesky-web && \
    npm install && \
    npm link itk-vtk-viewer

# Create the fix script with correct extension
WORKDIR /app/splash_flows_globus/orchestration/flows/bl832/view_recon_app
RUN echo '#!/usr/bin/env node\n\
const fs = require("fs");\n\
const path = require("path");\n\
\n\
const packageJsonPath = path.join("node_modules", "bluesky-web", "package.json");\n\
\n\
if (fs.existsSync(packageJsonPath)) {\n\
    console.log("Fixing bluesky-web package.json...");\n\
    const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));\n\
    \n\
    if (packageJson.exports && packageJson.exports["./style.css"] && !packageJson.exports["./dist/bluesky-web.css"]) {\n\
        packageJson.exports["./dist/bluesky-web.css"] = "./dist/bluesky-web.css";\n\
        fs.writeFileSync(packageJsonPath, JSON.stringify(packageJson, null, 2));\n\
        console.log("Successfully added missing export to bluesky-web package.json");\n\
    } else {\n\
        console.log("Package already has the required exports or structure is different than expected");\n\
    }\n\
} else {\n\
    console.log("bluesky-web package.json not found at", packageJsonPath);\n\
}\n' > fix-bluesky.cjs && chmod +x fix-bluesky.cjs

# Run the fix script
RUN node fix-bluesky.cjs

WORKDIR /app

# Copy startup script
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

# Create mount point for data directory
RUN mkdir -p /data

# Set environment variables
ENV DATA_DIR="/data"
ENV TILED_PORT=8000
ENV VIEWER_PORT=8082
ENV REACT_PORT=5174

# Expose ports
EXPOSE $TILED_PORT $VIEWER_PORT $REACT_PORT

# Set entrypoint
ENTRYPOINT ["/app/start.sh"]