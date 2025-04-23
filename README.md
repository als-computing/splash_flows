<p align="center">
  <img src="splash_flows_logo_text.png" alt="Arroyo Logo" width="150"/>
</p>

# Splash Flows Globus

This repo contains configuration and code for Prefect workflows to move data and run computing tasks.  Many of the workflows use Globus for data movement between local servers and back and forth to NERSC.

These flows can be run from the command line or built into a self-contained docker container.

## Getting started

### Clone this repo and set up the python enviornment:
```
$   git clone git@github.com:als-computing/splash_flows_globus.git
$   cd splash_flows_globus
$   pip3 install -e .
```

###  Provide Prefect and Globus authentication in `.env`:

Use `.env.example` as a template.

```
GLOBUS_CLIENT_ID=<globus_client_id>                 # For Globus Transfer
GLOBUS_CLIENT_SECRET=<globus_client_secret>         # For Globus Transfer
GLOBUS_COMPUTE_CLIENT_ID=<globus_client_id>         # For ALCF Jobs
GLOBUS_COMPUTE_CLIENT_SECRET=<globus_client_secret> # For ALCF Jobs
GLOBUS_COMPUTE_ENDPOINT=<globus_compute_endpoint>   # For ALCF Jobs
PREFECT_API_URL=<url_of_prefect_server>             # For Prefect Flows
PREFECT_API_KEY=<prefect_client_secret>             # For Prefect Flows
SCICAT_API_URL=<url_of_scicat_api>                  # For SciCat Ingest
SCICAT_INGEST_USER=<scicat_ingest_user>             # For SciCat Ingest
SCICAT_INGEST_PASSWORD=<scicat_ingest_password>     # For SciCat Ingest
PATH_NERSC_CLIENT_ID=<path_nersc_client_id>         # For NERSC SFAPI, generate on https://iris.nersc.gov/
PATH_NERSC_PRI_KEY=<path_nersc_private_key>         # For NERSC SFAPI
```

## Current workflow overview and status:

| Name                         | Description                                                                         |  Status  | Notes |
|------------------------------|-------------------------------------------------------------------------------------|:--------:|-------|
| `move.py`                    | Move data from spot832 to data832, schedule pruning, and ingest into scicat         | Deployed | [Details](./docs/bl832_ALCF.md) |
| `prune.py`                   | Run data pruning flows as scheduled                                                 | Deployed |       |
| `alcf.py`                    | Run tomography reconstruction Globus Compute Flows at ALCF                          | Deployed |       |
| `nersc.py`                   | Run tomography reconstruction using SFAPI at NERSC                                  |    WIP   |       |
| `dispatcher.py`                                 | Dispatch flow to control beamline subflows                                          |    WIP   |       |
| `create_deployments_<bl>.sh`                    | Deploy functions as prefect flows. Run once, or after updating underlying flow code |          |       |
| `globus/flows.py` and `globus/transfer.py`      | Connect Python with globus API – could use better error handling                    |          |       |
| `scripts/check_globus_compute.py`               | Check if CC has access to compute endpoint and if it is available                   |          |       |
| `scripts/check_globus_transfer.py`              | Check if CC has r/w/d access to an endpoint. Also ability to delete data            |          |       |
| `source scripts/login_to_globus_and_prefect.py` | Login to globus/prefect in current terminal from .env file                          |          |       |
| `init_<data_task>_globus_flow.py`               | Register and update globus flow UUIDs in Prefect                                    |          |       |
| `init_<data_task>_globus_flow.py`               | Register and update globus flow UUIDs in Prefect                                    |          |       |
| `orchestration/tests/<pytest_scripts>.py`       | Test scripts using pytest                                                           |          |       |

## Further development

### Globus collection endpoints are defined in `config.yml`:

```
globus:
  globus_endpoints:
    spot832:
      root_path: /
      uri: spot832.lbl.gov
      uuid: 44ae904c-ab64-4145-a8f0-7287de38324d
```

### Prefect workflows are deployed using the `create_deployments_[name].sh` scripts.

These are meant to be run on `flow-prd`, in `bl832_agent` with properly set `.env` variables (i.e. prefect id/secret, globus id/secret, ..)

General anatomy of a file:

```
prefect deployment build <path_of_file>:<prefect_function> -n 'name_of_the_workflow' -q <tag>
prefect deployment apply <prefect_function>-deployment.yaml
```

Example:  The following creates a Prefect workflow for the function of `process_new_file` in file of `./orchestration/flows/bl7012/move.py`

```
prefect deployment build ./orchestration/flows/bl7012/move.py:process_new_file -n 'process_newdata7012' -q bl7012
prefect deployment apply process_new_file-deployment.yaml
```

## Starting a Prefect workflow manually

Below is the command to start the Prefect workflow:
```
python -m orchestration.flows.bl7012.move <Relative path of file respect to the root_path defined in Globus endpoint>
```

## Submitting workflow via Prefect API

An example is shown `example.ipynb` to submit a PREFECT workflow to PREFECT server. 

Once the job is submitted, a workflow agent is needed to work on jobs in queue. A workflow agent can be launched by:

```
prefect agent start -q <name-of-work-queue>
```

It requires to have the `PREFECT_API_URL` and `PREFECT_API_KEY` stored as an environment variables, such that the agent knows where to get the work queue. Once the agent is launched, the following message indicates where the agent is currently listening to.

```
Starting v2.7.9 agent connected to https://.../api...

  ___ ___ ___ ___ ___ ___ _____     _   ___ ___ _  _ _____
 | _ \ _ \ __| __| __/ __|_   _|   /_\ / __| __| \| |_   _|
 |  _/   / _|| _|| _| (__  | |    / _ \ (_ | _|| .` | | |
 |_| |_|_\___|_| |___\___| |_|   /_/ \_\___|___|_|\_| |_|


Agent started! Looking for work from queue(s): <name-of-work-queue>...
```

## More specific documentation can be found in the `docs` folder:

* ["Globus configuration"](./docs/globus.md)
* ["8.3.2 ALCF Globus Flow And Reconstruction Setup"](./docs/bl832_ALCF.md)
* ["Data movement for BL7012"](./docs/bl7012.md)