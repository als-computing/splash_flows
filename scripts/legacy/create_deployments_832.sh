export $(grep -v '^#' .env | xargs)


# Create work pools. If a work pool already exists, it will throw a warning but that's no problem
prefect work-pool create 'new_file_832_flow_pool'
prefect work-pool create 'new_file_832_prune_pool'
prefect work-pool create 'scicat_ingest_pool'

# new_file_832_flow_pool
    # in docker-compose.yaml:
    # command: prefect agent start --pool "new_file_832_flow_pool"
    
# file transfer from data832 to nersc832 with Grafana Monitoring enabled
prefect deployment build ./orchestration/flows/bl832/move.py:test_transfers_832_grafana -n test_transfers_832_grafana -p new_file_832_flow_pool -q test_transfers_832_queue
prefect deployment apply test_transfers_832_grafana-deployment.yaml

prefect deployment build ./orchestration/flows/bl832/move.py:process_new_832_file -n new_file_832 -p new_file_832_flow_pool -q new_file_832_queue
prefect deployment apply process_new_832_file-deployment.yaml

prefect deployment build ./orchestration/flows/bl832/move.py:test_transfers_832 -n test_transfers_832 -p new_file_832_flow_pool -q test_transfers_832_queue
prefect deployment apply test_transfers_832-deployment.yaml

# new_file_832_prune_pool
    # in docker-compose.yaml:
    # command: prefect agent start --pool "new_file_832_prune_pool"

prefect deployment build ./orchestration/flows/bl832/prune.py:prune_spot832 -n prune_spot832 -p new_file_832_prune_pool -q prune_spot832_queue
prefect deployment apply prune_spot832-deployment.yaml

prefect deployment build ./orchestration/flows/bl832/prune.py:prune_data832 -n prune_data832 -p new_file_832_prune_pool -q prune_data832_queue
prefect deployment apply prune_data832-deployment.yaml

# scicat_ingest_pool
    # in docker-compose.yaml:
    # command: prefect agent start --pool "scicat_ingest_pool"

prefect deployment build ./orchestration/flows/scicat/ingest.py:ingest_dataset -n ingest_dataset -p scicat_ingest_pool -q ingest_dataset_queue
prefect deployment apply ingest_dataset-deployment.yaml