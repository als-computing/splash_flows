#!/usr/bin/env bash

# Create deployments for the 733 flows in the legacy 2.19 version of Prefect

export $(grep -v '^#' .env | xargs)


# Create work pools. If a work pool already exists, it will throw a warning but that's no problem
prefect work-pool create 'dispatcher_733_pool'
prefect work-pool create 'new_file_733_pool'
prefect work-pool create 'prune_733_pool'


# dispatcher_733_pool
    # in docker-compose.yaml:
    # command: prefect agent start --pool "dispatcher_733_pool"
prefect deployment build ./orchestration/flows/bl733/dispatcher.py:dispatcher -n run_733_dispatcher -p dispatcher_733_pool -q dispatcher_733_queue
prefect deployment apply dispatcher-deployment.yaml


# new_file_733_pool
    # in docker-compose.yaml:
    # command: prefect agent start --pool "new_file_733_pool"
prefect deployment build ./orchestration/flows/bl733/move.py:process_new_733_file -n new_file_733 -p new_file_733_pool -q new_file_733_queue
prefect deployment apply process_new_733_file-deployment.yaml


# TODO: Wait for PR #62 to be merged and use the new prune_controller
# prune_733_pool
    # in docker-compose.yaml:
    # command: prefect agent start --pool "prune_733_pool"

prefect deployment build ./orchestration/flows/bl733/move.py:prune -n prune_data733 -p prune_733_pool -q prune_733_queue
prefect deployment apply prune_data733-deployment.yaml
