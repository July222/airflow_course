from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta


# we create a custom operator in order to make some parameters templated
class CustomPostgresOperator(PostgresOperator):

    template_fields = ('sql', 'parameters')


def _stage1(ti):
    partner_name = 'netflix'
    partner_path = '/partners/netflix'
    # ti.xcom_push(key="partner_name", value=partner_name)
    return {'partner_name': partner_name, 'partner_path': partner_path}

def _stage2(ti):
    # partner_name = ti.xcom_pull(key="partner_name", task_ids='stage1')
    partner_settings = ti.xcom_pull(task_ids='stage1')
    print(partner_settings['partner_name'])


with DAG(
        dag_id="my_dag",  # DAG id which must be unique
        description="this is test DAG",
        start_date=datetime(2022, 1, 5),   # the date when DAG starts being scheduled. Can be defined for each task separately. With catchup=True backfill process for old dates can start
        schedule_interval="@daily",       # interval of time fromthe min (start_date) at which DAG is triggered. We can define it by cron expression or timedelta
        # schedule_interval=timedelta(minutes=5),  # we can define by cron expression or timedelta
        dagrun_timeout=timedelta(minutes=10),  # if the DAG takes more than 0 minutes to complete, than it failes
        tags=["test"],      # tags help filter DAGs
        catchup=False,       # do we need to do rerun/backfill automatically? Best practise is to set it to False
        max_active_runs=5   # how many DAG runs can be executed at the same time
) as dag:


    stage1 = PythonOperator(
        task_id="stage1",
        python_callable=_stage1)

    stage2 = PythonOperator(
        task_id="stage2",
        python_callable=_stage2)

    stage1 >> stage2