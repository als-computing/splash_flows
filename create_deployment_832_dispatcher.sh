export $(grep -v '^#' .env | xargs)


prefect work-pool create 'dispatcher_pool'
prefect deployment build ./orchestration/flows/bl832/dispatcher.py:dispatcher -n run_832_dispatcher -q bl832 -p dispatcher_pool
prefect deployment apply dispatcher-deployment.yaml

prefect work-pool create 'hpss_pool'
prefect deployment build ./orchestration/flows/bl832/dispatcher.py:archive_832_project_dispatcher -n run_archive_832_project_dispatcher -q hpss_dispatcher_queue -p hpss_pool
prefect deployment apply archive_832_project_dispatcher-deployment.yaml

prefect deployment build ./orchestration/flows/bl832/dispatcher.py:archive_832_projects_from_previous_cycle_dispatcher -n run_archive_832_projects_from_previous_cycle_dispatcher -q hpss_dispatcher_queue -p hpss_pool
prefect deployment apply archive_832_projects_from_previous_cycle_dispatcher-deployment.yaml

prefect deployment build ./orchestration/flows/bl832/dispatcher.py:archive_all_832_raw_projects_dispatcher -n run_archive_all_832_raw_projects_dispatcher -q hpss_dispatcher_queue -p hpss_pool
prefect deployment apply archive_all_832_raw_projects_dispatcher-deployment.yaml