# -*- coding: utf-8 -*-

from builtins import range
from datetime import timedelta, datetime
import pendulum

from dateutil import tz

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.macros import dateutil

def print_the_new_timezone(**context):
    execution_date = context['ts']

    print("execution_date in ISO Format: {}".format(execution_date))

    # Convert UTC execution date to US/Pacific
    local_tz = pendulum.timezone("US/Pacific")
    new_datetime_object = local_tz.convert(execution_date)
    print("Converted datetime: {}".format(converted_datetime))

local_tz = pendulum.timezone("US/Pacific")

args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 5, 3, tzinfo=local_tz),
}

dag = DAG(
    dag_id='test_bash_template_dag',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
)

run_this_last = DummyOperator(
    task_id='run_this_last',
    dag=dag,
)

test_echo_command = BashOperator(
    task_id='test_echo_command_test',
    bash_command="echo \"Execution Date :\" {{ ds }}",
    dag=dag,
)

test_datetime_macro = BashOperator(
    task_id='test_datetime_macro_task',
    bash_command="echo {{ execution_date.replace(tzinfo=macros.dateutil.tz.gettz('US/Pacific')) }}",
    dag=dag,
)

test_python_op = PythonOperator(
    task_id='print_the_new_timezone_task',
    provide_context=True,
    python_callable=print_the_new_timezone,
    dag=dag,
)

test_echo_command >> test_python_op >> test_datetime_macro >> run_this_last
