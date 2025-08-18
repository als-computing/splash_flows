export $(grep -v '^#' .env | xargs)


prefect work-pool create 'dispatcher_pool'
prefect deployment build ./orchestration/flows/bl832/dispatcher.py:dispatcher -n run_832_dispatcher -q bl832 -p dispatcher_pool
prefect deployment apply dispatcher-deployment.yaml