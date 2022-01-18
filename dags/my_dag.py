from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta


# we create a custom operator in order to make some parameters templated
class CustomPostgresOperator(PostgresOperator):

    template_fields = ('sql', 'parameters')


def _extract(responsible_name):
    print(responsible_name)


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

    extract = PythonOperator(
        task_id="extract",
        python_callable=_extract,
        # op_args=[Variable.get('my_dag_responsible', deserialize_json=True)['name']]    # deserialize_json=True is needed to parse a JSON variable
        op_args=["{{var.json.my_dag_responsible.name}}"] # unlikely the commented line, we get the variable value only when DAG runs (not in the parse time), so we don't create additional useless DB connections
    )

    fetching_data = CustomPostgresOperator(
        task_id="fetching_data",
        sql="sql/MY_REQUEST.sql",
        parameters={
            "next_ds": '{{next_ds}}',
            'prev_ds': '{{prev_ds}}',
            'responsible_name': '{{var.json.my_dag_responsible.name}}'    #  these variables will be fetched during run time, not parse time
        }
    )
