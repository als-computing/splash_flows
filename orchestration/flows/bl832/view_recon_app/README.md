# Tomography Reconstruction Visualizer

## (Powered by itk-vtk-viewer)

This web application allows users to load their reconstructed volumes using Tiled Browser (in development) into the itk-vtk-viewer widget, all within the same web page.

TODO:

*   Add a 'listener' mode that loads in the most recent tomographic reconstruction once processing is completed and the data is available.

---

# Installation via Docker (Recommended)

Before you begin, make sure you have Docker installed on your machine.

Then, clone this repository and then navigate to the `view_recon_app` directory.

  
`cd splash_flows_globus/orchestration/flows/bl832/view_recon_app`

Create a new file `.env` in that folder (or rename `.env.example` ) and add the following line (make sure to update the path to a real location):  
 

`DATA_PATH=/full/path/to/your/reconstructions/folder/no/trailing/slash`  
 

Now we're ready to build the Docker container.

`docker compose up -d`

This will build the container and start the application in the background.  
Before we can use the app, we need to authenticate with Bluesky Tiled.

In the same folder, run:

`docker compose logs`

Look for this part of the logs:  
 

```
tomography-visualizer-1  | Starting Tiled server with data from /data on port 8000...
tomography-visualizer-1  | Waiting for Tiled server to start...
tomography-visualizer-1  | Creating catalog database at /tmp/tmpsmzvu3_x/catalog.db
tomography-visualizer-1  | 
tomography-visualizer-1  |     Tiled server is running in "public" mode, permitting open, anonymous access
tomography-visualizer-1  |     for reading. Any data that is not specifically controlled with an access
tomography-visualizer-1  |     policy will be visible to anyone who can connect to this server.
tomography-visualizer-1  | 
tomography-visualizer-1  | 
tomography-visualizer-1  |     Navigate a web browser or connect a Tiled client to:
tomography-visualizer-1  | 
tomography-visualizer-1  |     http://0.0.0.0:8000?api_key=4c138df71f1887db6b3a4f308888e41b074869dda04ca93fb40907b89b49134d
tomography-visualizer-1  | 
tomography-visualizer-1  | 
tomography-visualizer-1  |     Because this server is public, the '?api_key=...' portion of
tomography-visualizer-1  |     the URL is needed only for _writing_ data (if applicable).
```

Navigate to the url provided in your web browser: `http://0.0.0.0:8000?api_key=<thiskeyisunique>.`  
Now we can start the View Recon App and have access to our data.  
  
In your browser, navigate to: http://localhost:5174  
  
Voila!

---

# Installation from Scratch

## `Bluesky Tiled`

Host your reconstructed data using `Tiled`, which we use to connect data servers to front end applications such as this one.

#### Prepare environment

`Tiled` support for `Zarr` is a work in progress, but there is a specific branch you can use for this application:

*   `add-zarr-forked` branch: https://github.com/davramov/tiled/tree/add-zarr-forked
*   This was a small addition to this PR on the source repo: https://github.com/bluesky/tiled/pull/774

To install this version of Tiled, I recommend creating a new Conda environment and following the "[Install Tiled from Source](https://blueskyproject.io/tiled/tutorials/installation.html#source)" instructions:

```
conda create env -n "tiled_zarr_env python=3.12" 
conda activate tiled_zarr_env
```

#### Clone and install repository

Instead of installing the main version, use this fork with Zarr support:

```
git clone -b add-zarr-forked https://github.com/davramov/tiled.git`
cd tiled
pip install -e ".[all]"
```

#### Start Tiled

Open a new terminal, and either navigate to the directory containing your zarr projects, or specify the full path directly:

```
cd [go/to/your/zarr/projects/]
TILED_ALLOW_ORIGINS="http://localhost:3000 http://localhost:5174 http://localhost:8082" tiled serve directory "data/tomo/scratch/" --public --verbose
```

If that is successful, you should see something like this:

```
(tiled_zarr_env) you@your-computer tiled_zarr % TILED_ALLOW_ORIGINS="http://localhost:3000 http://localhost:5174 http://localhost:8082" tiled serve directory "data/tomo/scratch/" --public --verbose
Creating catalog database at /var/folders/7t/17b_zxx55jnggw80_6672tbh0000gn/T/tmp94wbqw7y/catalog.db

    Tiled server is running in "public" mode, permitting open, anonymous access
    for reading. Any data that is not specifically controlled with an access
    policy will be visible to anyone who can connect to this server.


    Navigate a web browser or connect a Tiled client to:

    http://127.0.0.1:8000?api_key=ee7caca056af09c62993ffa789689bf181d41a4e885544c070e68b32339ed1c0


    Because this server is public, the '?api_key=...' portion of
    the URL is needed only for _writing_ data (if applicable).


[-] INFO:     Started server process [15895]
[-] INFO:     Waiting for application startup.
Tiled version 0.1.dev2517+g202f10e
[-] INFO:     Application startup complete.
[-] INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
[58c42c2098d320fd] 127.0.0.1:61412 (unset) - "GET /api/v1/ HTTP/1.1" 200 OK
[74cb5a4350af0bb9] 127.0.0.1:61412 (unset) - "GET /api/v1/metadata/?include_data_sources=false HTTP/1.1" 200 OK
Server is up. Indexing files in data/tomo/scratch/...
  Overwriting '/'
```

The important thing to grab here is the URL with the api\_key:

`**http://127.0.0.1:8000?api_key=ee7caca056af09c62993ffa789689bf181d41a4e885544c070e68b32339ed1c0**`

In your web browser, open the URL you see in your terminal to activate the Tiled session. The Zarr files that are indexed here and viewable in the main Tiled UI are loadable from the Tiled Browser widget in the React App.

## `itk-vtk-viewer`

#### Install

[Official Documentation](https://kitware.github.io/itk-vtk-viewer/docs/cli.html)

[Install or update Node.js](https://nodejs.org/en/download)

Then, install `itk-vtk-viewer`

**npm version**

```
npm install itk-vtk-viewer -g
```

**specific version**  
The `itk-vtk-viewer` interface is customizable, so you can install a different version following these steps:

#### Run

Once you have a version of `itk-vtk-viewer` installed in your environment, you can start it in the command line with the following command:

```
itk-vtk-viewer --port 8082
```

We specify `--port 8082`, as this is what the React App is configured to listen to by default.

### Start the `React` App

Run in your terminal

```
cd /orchestration/flows/bl832/view_recon_app/
npm run dev
```

```
  VITE v6.0.11  ready in 139 ms

  ➜  Local:   http://localhost:5174/
  ➜  Network: use --host to expose
  ➜  press h + enter to show help
```

Navigate to http://localhost:5174/ in your web browser