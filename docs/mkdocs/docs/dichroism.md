# Dichroism Beamline Flows
This page documents the workflows supported by Splash Flows for the ALS Dichroism beamlines:

- [Beamline 4.0.2](https://als.lbl.gov/beamlines/4-0-2/)
- [Beamline 6.3.1](https://als.lbl.gov/beamlines/6-3-1/)

**Data at Dichroism Beamlines**
These beamlines generate X-ray magnetic circular dichroism (XMCD) and X-ray magnetic linear dichroism (XMLD) spectroscopy data.

## File Watcher
There is a file watcher on the acquisition system that listens for new scans that have finished writing to disk. From there, a Prefect Flow we call dispatcher kicks off the downstream steps:

Copy scans in real time from a Globus collection on the compute-dtn server to NERSC CFS using Globus Transfer.
Copy project data to NERSC HPSS for long-term storage (TBD).
Ingest into SciCat (TBD).
Schedule data pruning from compute-dtn and NERSC CFS.

## Prefect Configuration

### Registered Flows
#### dispatcher.py
The Dispatcher Prefect Flow manages the logic for handling the order and execution of data tasks. Once a new file is written, the dispatcher() Flow is called with either BL402 or BL631 as a parameter to specify the beamline. The dispatcher handles the synchronous call to the appropriate move task.
move.py
Contains separate move tasks/flows for each beamline:

- process_new_402_file: Flow to process a new file at BL 4.0.2
- process_new_631_file: Flow to process a new file at BL 6.3.1

Each flow performs the following steps:

Copy the file from compute-dtn to NERSC CFS and ingest the file path and metadata into SciCat.
Schedule pruning from compute-dtn.
Copy the file from NERSC CFS to NERSC HPSS. Ingest the archived file path in SciCat.
Schedule pruning from NERSC CFS.

### Work Pools and Queues
The following work pools are defined in orchestration/flows/dichroism/prefect.yaml:
DeploymentWork PoolWork Queuerun_dichroism_dispatcherdispatcher_dichroism_pooldispatcher_402_queue / dispatcher_631_queuenew_file_402new_file_dichroism_poolnew_file_402_queuenew_file_631new_file_dichroism_poolnew_file_631_queuetest_transfers_dichroismnew_file_dichroism_pooltest_transfers_dichroism_queue
Both beamlines share the same work pools but use separate queues for fine-grained control.
Configuration
Globus Endpoints
Endpoints are defined in config.yml:
yamldata402:
  root_path: /path/to/compute-dtn/4.0.2/data
  uri: compute-dtn.als.lbl.gov
  uuid: <endpoint-uuid>
  name: data402

nersc402:
  root_path: /global/cfs/cdirs/als/data_mover/4.0.2
  uri: nersc.gov
  uuid: <nersc-endpoint-uuid>
  name: nersc402

data631:
  root_path: /path/to/compute-dtn/6.3.1/data
  uri: compute-dtn.als.lbl.gov
  uuid: <endpoint-uuid>
  name: data631

nersc631:
  root_path: /global/cfs/cdirs/als/data_mover/6.3.1
  uri: nersc.gov
  uuid: <nersc-endpoint-uuid>
  name: nersc631
Environment Variables
Required environment variables (set in .env or container environment):
bashGLOBUS_CLIENT_ID=<globus_client_id>
GLOBUS_CLIENT_SECRET=<globus_client_secret>
PREFECT_API_URL=<url_of_prefect_server>
PREFECT_API_KEY=<prefect_api_key>
Deployment
Register Flows
Using the init script with Docker:
bashBEAMLINE=dichroism ./init_work_pools.py
Or deploy manually:
bashprefect deploy --prefect-file orchestration/flows/dichroism/prefect.yaml --all
Start Workers
bashprefect worker start --pool "dispatcher_dichroism_pool"
prefect worker start --pool "new_file_dichroism_pool"
VM Details
The computing backend runs on a VM managed by ALS IT staff.
flow-dichroism.als.lbl.govShareArtifactsDownload allDichroism readmeDocument · MD Project contentsplash_flowsCreated by youals-computing/splash_flowsmainGITHUB