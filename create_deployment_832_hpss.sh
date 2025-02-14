export $(grep -v '^#' .env | xargs)


prefect work-pool create 'hpss_pool'

prefect deployment build ./orchestration/flows/bl832/hpss.py:cfs_to_hpss_flow -n cfs_to_hpss_flow -q cfs_to_hpss_queue -p hpss_poolr_pool
prefect deployment apply cfs_to_hpss_flow-deployment.yaml