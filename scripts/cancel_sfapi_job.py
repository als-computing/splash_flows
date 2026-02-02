"""
Script to manage NERSC SLURM jobs via SFAPI.

Usage:

# List all jobs
python sfapi_jobs.py list
python sfapi_jobs.py -u dabramov list

# Cancel a specific job
python sfapi_jobs.py cancel 47470003

# Cancel all jobs for a user
python sfapi_jobs.py cancel-all
python sfapi_jobs.py -u dabramov cancel-all
"""

from dotenv import load_dotenv
import argparse
import json
import logging
import os

from authlib.jose import JsonWebKey
from sfapi_client import Client
from sfapi_client.compute import Machine


load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

client_id_path = os.getenv("PATH_NERSC_CLIENT_ID")
client_secret_path = os.getenv("PATH_NERSC_PRI_KEY")

if not client_id_path or not client_secret_path:
    logger.error("NERSC credentials paths are missing.")
    raise ValueError("Missing NERSC credentials paths.")
if not os.path.isfile(client_id_path) or not os.path.isfile(client_secret_path):
    logger.error("NERSC credential files are missing.")
    raise FileNotFoundError("NERSC credential files are missing.")

client_id = None
client_secret = None
with open(client_id_path, "r") as f:
    client_id = f.read()

with open(client_secret_path, "r") as f:
    client_secret = JsonWebKey.import_key(json.loads(f.read()))


def list_jobs(user: str = "alsdev"):
    """List all jobs for a user."""
    with Client(client_id, client_secret) as client:
        perlmutter = client.compute(Machine.perlmutter)
        jobs = perlmutter.jobs(user=user)
        if not jobs:
            logger.info(f"No jobs found for user: {user}")
            return
        for job in jobs:
            logger.info(f"Job {job.jobid}: {job.name} - {job.state}")


def cancel_job(jobid: str):
    """Cancel a specific job by ID."""
    with Client(client_id, client_secret) as client:
        perlmutter = client.compute(Machine.perlmutter)
        job = perlmutter.job(jobid=jobid)
        logger.info(f"Cancelling job: {job.jobid}")
        job.cancel(wait=True)
        logger.info(f"Job {job.jobid} cancelled, state: {job.state}")


def cancel_all_jobs(user: str = "alsdev"):
    """Cancel all jobs for a user."""
    with Client(client_id, client_secret) as client:
        perlmutter = client.compute(Machine.perlmutter)
        jobs = perlmutter.jobs(user=user)
        if not jobs:
            logger.info(f"No jobs found for user: {user}")
            return
        for job in jobs:
            logger.info(f"Cancelling job: {job.jobid} ({job.name})")
            job.cancel()
        logger.info(f"Cancelled {len(jobs)} jobs")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage NERSC SLURM jobs via SFAPI")
    parser.add_argument("--user", "-u", default="alsdev", help="Username for job queries")

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # List jobs
    list_parser = subparsers.add_parser("list", help="List all jobs for a user")

    # Cancel specific job
    cancel_parser = subparsers.add_parser("cancel", help="Cancel a specific job")
    cancel_parser.add_argument("jobid", help="Job ID to cancel")

    # Cancel all jobs
    cancel_all_parser = subparsers.add_parser("cancel-all", help="Cancel all jobs for a user")

    args = parser.parse_args()

    if args.command == "list":
        list_jobs(user=args.user)
    elif args.command == "cancel":
        cancel_job(jobid=args.jobid)
    elif args.command == "cancel-all":
        cancel_all_jobs(user=args.user)
    else:
        parser.print_help()
