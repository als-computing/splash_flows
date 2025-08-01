This branch documents the code used to present Prefect Flow timings in the XLOOP Workshop at Supercomputing '25 submission "Accelerating Advanced Light Source Science Through Multi-Facility HPC Workflows"

- `login_to_prefect.sh` reads Prefect key and API url from .env file and sets them in the active environment.
- `.env.example` shows the expected Prefect variables.
- `main.py` is the main entrypoint, used to define the Flows and run the query.
- `requirements.txt` supplies the two dependencies required by this program (prefect and pandas).
- `*.csv` and `.tex` files are the outputs of this program.
