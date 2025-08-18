export $(grep -v '^#' .env | xargs)


# create 'alfc_flow_pool'
prefect work-pool create 'alcf_flow_pool'
# create 'aclf_prune_pool'
prefect work-pool create 'alcf_prune_pool'

# alcf_flow_pool
    # in docker-compose.yaml:
    # command: prefect agent start --pool "alcf_flow_pool"
prefect deployment build ./orchestration/flows/bl832/alcf.py:alcf_recon_flow -n alcf_recon_flow -p alcf_flow_pool -q alcf_recon_flow_queue
prefect deployment apply alcf_recon_flow-deployment.yaml

# alcf_prune_pool
    # in docker-compose.yaml:
    # command: prefect agent start --pool "alcf_prune_pool"
prefect deployment build ./orchestration/flows/bl832/prune.py:prune_alcf832_raw -n prune_alcf832_raw -p alcf_prune_pool -q prune_alcf832_raw_queue
prefect deployment apply prune_alcf832_raw-deployment.yaml


prefect deployment build ./orchestration/flows/bl832/prune.py:prune_alcf832_scratch -n prune_alcf832_scratch -p alcf_prune_pool -q prune_alcf832_scratch_queue
prefect deployment apply prune_alcf832_scratch-deployment.yaml


prefect deployment build ./orchestration/flows/bl832/prune.py:prune_data832_raw -n prune_data832_raw -p alcf_prune_pool -q prune_data832_raw_queue
prefect deployment apply prune_data832_raw-deployment.yaml


prefect deployment build ./orchestration/flows/bl832/prune.py:prune_data832_scratch -n prune_data832_scratch -q bl832 -p alcf_prune_pool -q prune_data832_scratch_queue
prefect deployment apply prune_data832_scratch-deployment.yaml


# prefect deployment build ./orchestration/flows/bl832/prune.py:prune_nersc832_alsdev_scratch -n prune_nersc832_alsdev_scratch -q bl832 -p alcf_prune_pool -q prune_nersc832_alsdev_scratch_queue
# prefect deployment apply prune_nersc832_alsdev_scratch-deployment.yaml