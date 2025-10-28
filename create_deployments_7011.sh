export $(grep -v '^#' .env | xargs)


# Create work pools. If a work pool already exists, it will throw a warning but that's no problem
prefect work-pool create 'dispatcher_7011_flow_pool'
prefect work-pool create 'new_file_7011_flow_pool'
# prefect work-pool create 'new_file_7011_prune_pool'

# dispatcher_7011_flow_pool
    # in docker-compose.yaml:
    # command: prefect agent start --pool "dispatcher_7011_flow_pool"
prefect deployment build ./orchestration/flows/bl7011/dispatcher.py:dispatcher -n run_733_dispatcher -p dispatcher_pool -q dispatcher_733_queue
prefect deployment apply dispatcher-deployment.yaml

# new_file_7011_flow_pool
    # in docker-compose.yaml:
    # command: prefect agent start --pool "new_file_7011_flow_pool"
prefect deployment build ./orchestration/flows/bl7011/move.py:process_new_7011_file -n new_file_7011 -p new_file_7011_flow_pool -q new_file_7011_queue
prefect deployment apply process_new_7011_file-deployment.yaml


# TODO: Wait for PR #62 to be merged and use the new prune_controller
# new_file_7011_prune_pool
    # in docker-compose.yaml:
    # command: prefect agent start --pool "new_file_7011_prune_pool"

# prefect deployment build ./orchestration/flows/bl7011/prune.py:prune_data7011 -n prune_data7011 -p new_file_7011_prune_pool -q prune_data7011_queue
# prefect deployment apply prune_data7011-deployment.yaml
