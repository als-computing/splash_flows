# Welcome to Splash Flows Globus

This repository contains code for automating workflows at several Advanced Light Source beamlines.

## Project layout
Here is the current structure of this repository:

```
.
└── splash_flows_globus
    ├── COPYWRITE
    ├── Dockerfile
    ├── LICENSE
    ├── Makefile
    ├── README.md
    ├── config.yml
    ├── create_deployment_832_dispatcher.sh
    ├── create_deployments_7012.sh
    ├── create_deployments_832.sh
    ├── create_deployments_832_alcf.sh
    ├── create_deployments_832_nersc.sh
    ├── docs
    │   ├── bl7012.md
    │   ├── bl832_ALCF.md
    │   ├── globus.md
    │   └── mkdocs
    │       ├── docs
    │       │   ├── configuration.md
    │       │   ├── getting_started.md
    │       │   ├── glossary.md
    │       │   ├── index.md
    │       │   ├── install.md
    │       │   ├── orchestration.md
    │       │   └── troubleshooting.md
    │       └── mkdocs.yml
    ├── examples
    │   ├── launch_ptycho.py
    │   ├── sfapi_NerscClient_example.ipynb
    │   └── start_job.ipynb
    ├── orchestration
    │   ├── __init__.py
    │   ├── _tests
    │   │   ├── __init__.py
    │   │   ├── conftest.py
    │   │   ├── test_config.py
    │   │   ├── test_config.yml
    │   │   ├── test_globus.py
    │   │   ├── test_globus_flow.py
    │   │   ├── test_sfapi_flow.py
    │   │   └── test_transfer_controller.py
    │   ├── config.py
    │   ├── flows
    │   │   ├── __init__.py
    │   │   ├── bl7012
    │   │   │   ├── __init__.py
    │   │   │   ├── config.py
    │   │   │   ├── example.ipynb
    │   │   │   ├── move.py
    │   │   │   ├── move_recon.py
    │   │   │   ├── ptycho_jobscript.py
    │   │   │   └── ptycho_nersc.py
    │   │   ├── bl733
    │   │   │   └── move_733.py
    │   │   ├── bl832
    │   │   │   ├── __init__.py
    │   │   │   ├── alcf.py
    │   │   │   ├── config.py
    │   │   │   ├── dispatcher.py
    │   │   │   ├── ingest_tomo832.py
    │   │   │   ├── job_controller.py
    │   │   │   ├── move.py
    │   │   │   ├── nersc.py
    │   │   │   ├── olcf.py
    │   │   │   └── prune.py
    │   │   └── scicat
    │   │       ├── __init__.py
    │   │       ├── ingest.py
    │   │       └── utils.py
    │   ├── globus
    │   │   ├── __init__.py
    │   │   ├── flows.py
    │   │   └── transfer.py
    │   ├── nersc.py
    │   ├── prefect.py
    │   └── transfer_controller.py
    ├── pytest.ini
    ├── requirements-dev.txt
    ├── requirements.txt
    ├── scripts
    │   ├── Tomopy_for_ALS.ipynb
    │   ├── cancel_sfapi_job.py
    │   ├── check_globus_compute.py
    │   ├── check_globus_transfer.py
    │   ├── init_tiff_to_zarr_globus_flow.py
    │   ├── init_tomopy_globus_flow.py
    │   ├── login_to_globus_and_prefect.sh
    │   └── polaris
    │       ├── globus_reconstruction.py
    │       └── tiff_to_zarr.py
    ├── setup.cfg
    └── setup.py
```