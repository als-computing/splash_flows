# Common Infrastructure

## Overview
The common infrastructure for this project includes:
- **Shared Code**: There are general functions and classes used across beamline workflows to reduce code duplication.
- **Beamline Specific Implementation Patterns**: We organize each beamline's implementation in a similar way, making it easier to understand and maintain.

## Shared Code
Shared code is organized into modules that can be imported in beamline specific implementations. Key modules include:
- **`orchestration/config.py`**
    - Contains an Abstract Base Class (ABC) called `BeamlineConfig()` which serves as the base for all beamline-specific configuration classes. It uses the `Dynaconf` package to load the configuration file,`config.yml`, which contains information about endpoints, containers, and more.
- **`orchestration/transfer_endpoints.py`**
    - Contains an ABC called `TransferEndpoint()`, which is extended by `FileSystemEndpoint`, `HPSSEndpoint` and `GlobusEndpoint`. These definitions are used to enforce typing and ensure the correct transfer and pruning implmentation are used.
- **`orchestration/transfer_controller.py`**:
    - Contains an ABC called `TransferController()` with specific implementations for Globus, Local File Systems, and NERSC HPSS.
- **`orchestration/prune_controller.py`**
    - This module is responsible for managing the pruning of data off of storage systems. It uses a configurable retention policy to determine when to remove files. It contains an ABC called `PruneController()` that is extended by specific implementations for `FileSystemEndpoint`, `GlobusEndpoint`, and `HPSSEndpoint`.
- **`orchestration/sfapi.py`**: Create an SFAPI Client to launch remote jobs at NERSC.
- **`orchestration/flows/scicat/ingest.py`**: Ingests datasets into SciCat, our metadata management system.

## Beamline Specific Implementation Patterns
In order to balance generalizability, maintainability, and scalability of this project to multiple beamlines, we try to organize specific implementations in a similar way. We keep specific implementaqtions in the directory `orchestration/flows/bl{beamline_id}/`, which generally contains a few things:
- **`config.py`**
    - Extend `BeamlineConfig()` from `orchestration/config.py` for specific implementations (e.g. `Config832`, `Config733`, etc.) This ensures only the relevant beamline specific configurations are used in each case.
- **`dispatcher.py`**
    - This script is the starting point for each beamline's data transfer and analysis workflow. The Prefect Flow it contains is generally invoked by a File Watcher script on the beamline computer. The Dispatcher contains the logic for calling subflows, ensures that steps are completed in the correct order, and prevents subsequent steps from being called if there is a failure along the way.
- **`move.py`**
    - This script is usually the first one the Dispatcher calls synchronously, and contains the logic for immediately moving data, scheduling pruning flows, and ingesting into SciCat. Downstream steps typically rely on this action completing first.
- **`job_controller.py`**
    - For beamlines that trigger remote analysis workflows, the `JobController()` ABC allows us to define HPC or machine specific implementations, which may differ in how code can be deployed. For example, it can be extended to define how to run tomography reconstruction at ALCF and NERSC.
- **`{hpc}.py`**
    - We separate HPC implementations for `JobController()` in their own files.
- **`hpss.py`**
    - We define HPSS transfers for each beamline individually, as we provide different scheduling strategies based on the data throughput of each endstation.
- **`ingest.py`**
    - This is where we define SciCat implementations for each beamline, as each technique will have specific metadata fields that are important to capture.

## Testing
We write Unit Tests using [pytest](https://pytest.org/) for individual components, which can be found in `orchestration/_tests/`. We run these tests as part of our Github Actions.

## CI/CD
The project is integrated with [GitHub Actions](https://github.com/features/actions) for continuous integration and deployment. The specifics for these can be found in `.github/workflows/`. The features we support here includes:

- **Automated Test Execution**: All the unit tests are run automatically with every Git Push.
- **Linting**: `flake8` is used to check for syntax and styling errors.
- **MkDocs**: The documentation site is automatically updated whenever a Pull Request is merged into the main branch.
- **Docker**: A Docker image is aumatically created and registered on the Github Container Repository (ghcr.io) when a new release is made.