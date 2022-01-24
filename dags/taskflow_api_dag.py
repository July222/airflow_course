from airflow.decorators import task, dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from typing import Dict


# we create a custom operator in order to make some parameters templated
class CustomPostgresOperator(PostgresOperator):

    template_fields = ('sql', 'parameters')

@task.python
def stage1():
    partner_name = 'netflix'
    partner_path = '/partners/netflix'
    # ti.xcom_push(key="partner_name", value=partner_name)
    return {'partner_name': partner_name, 'partner_path': partner_path}


@task.python
def stage2(partner_settings):
    # partner_name = ti.xcom_pull(key="partner_name", task_ids='stage1')
    print(partner_settings['partner_name'])


@task.python
def stage3() -> Dict[str, str]:     # in this case each dict value becomes a separate xcom value
    partner_name = 'amazon'
    partner_path = '/partners/amazon'
    return {'partner_name': partner_name, 'partner_path': partner_path}


@task.python
def stage4(partner_name, partner_path):
    return {'partner_name': partner_name, 'partner_path': partner_path}

@dag(
        description="this is test DAG",
        start_date=datetime(2022, 1, 5),   # the date when DAG starts being scheduled. Can be defined for each task separately. With catchup=True backfill process for old dates can start
        schedule_interval="@daily",       # interval of time fromthe min (start_date) at which DAG is triggered. We can define it by cron expression or timedelta
        # schedule_interval=timedelta(minutes=5),  # we can define by cron expression or timedelta
        dagrun_timeout=timedelta(minutes=10),  # if the DAG takes more than 0 minutes to complete, than it failes
        tags=["test"],      # tags help filter DAGs
        catchup=False,       # do we need to do rerun/backfill automatically? Best practise is to set it to False
        max_active_runs=5   # how many DAG runs can be executed at the same time
)
def my_dag_taskflow_api():      # the name of the function is dag_id

    stage2(stage1())   # dependencies between tasks will be generated automatically
    partner_settings = stage3()
    stage4(partner_settings['partner_name'], partner_settings['partner_path'])


dag = my_dag_taskflow_api()