# coding=utf-8
import json
import logging
import os
from subprocess import check_call

import pendulum
import yaml

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

from ray_perjob_operator import RayPerJobOperator

local_tz = pendulum.timezone("Asia/Shanghai")
start_date = datetime.now(tz=local_tz).replace(
    hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
HEADERS = {"Content-Type": "application/json"}
default_owner = "sombody@app.com;sombody_2@app.com"

k8s_namespace = Variable.get("RAY_KUBERNETES_NAMESPACE", "ray")
k8s_conn_id = Variable.get("KUBERNETES_CONN_ID", "kubernetes_default")


def to_yaml_list_with_indent(data, indent=6):
    yaml_str = yaml.dump(data, default_flow_style=False)
    indented_str = "".join(f"{' ' * indent}{line}\n"
                           for line in yaml_str.splitlines())
    return indented_str.strip()


default_args = {
    'owner': 'somebody',
    'depends_on_past': True,
    'email': ['somebody@app.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('triage_runner_jobs',
          default_args=default_args,
          start_date=start_date,
          user_defined_macros={
              'to_yaml_list_with_indent': to_yaml_list_with_indent,
          },
          schedule_interval='@daily')


def gzip_log(file_path, file_name):
    return check_call(['gzip', os.path.join(file_path, file_name)])


def on_failure_callback(context):
    if context is None:
        logging.info('context is none when on_failure_callback')
        return

    airflow_owner_mail = Variable.get(
        'AIRFLOW_OWNER_EMAIL',
        'some@app.com')
    ti = context['task_instance']
    task = context['task']

    # call other func to send alert to somebody
    message = ""
    logging.error(message)


def on_task_success_callback(context):
    if context is None:
        logging.info('context is none when on_failure_callback')
        return
    ti = context['task_instance']
    logging.info("task: %s execute succeed %s " % ti.task_id)


def create_external_task_sensor(task_id,
                                external_dag_id,
                                external_task_id,
                                execution_delta=None,
                                timeout=60 * 60 * 20):
    return ExternalTaskSensor(task_id=task_id,
                              external_dag_id=external_dag_id,
                              external_task_id=external_task_id,
                              dag=dag,
                              execution_delta=execution_delta,
                              on_failure_callback=on_failure_callback,
                              on_success_callback=on_task_success_callback,
                              timeout=timeout)


def create_ray_per_job_operator(task_id,
                                        dag,
                                        application_file: str,
                                        timeout,
                                        k8s_namespace,
                                        kubernetes_conn_id,
                                        on_success_callback=None,
                                        on_failure_callback=None,
                                        params=None,
                                        attach_log=True):
    """

    :param task_id:
    :param dag:
    :param application_file:
    :param timeout:
    :param k8s_namespace:
    :param params:
    :param kubernetes_conn_id:
    :param on_success_callback:
    :param on_failure_callback:
    :param attach_log:
    :return:
    """
    return RayPerJobOperator(task_id=task_id,
                               namespace=k8s_namespace,
                               application_file=application_file,
                               kubernetes_conn_id=kubernetes_conn_id,
                               do_xcom_push=True,
                               attach_log=attach_log,
                               timeout=timeout,
                               on_success_callback=on_success_callback,
                               on_failure_callback=on_failure_callback,
                               params=params,
                               dag=dag)


start_task = BashOperator(
    task_id="start_task",
    bash_command="for i in {1..180}; do echo ${i}; sleep 1; done",
    dag=dag)

wait_external_dag_task = create_external_task_sensor(
    task_id="wait_external_dag_task",
    external_dag_id="external_dag_id",
    external_task_id="external_task_id",
    execution_delta=None)

ray_per_job_runner = create_ray_per_job_operator(
    task_id="ray_per_job_runner",
    dag=dag,
    application_file="ray_yaml/per_job_template.yaml",
    timeout=3600,
    k8s_namespace=k8s_namespace,
    kubernetes_conn_id=k8s_conn_id,
    on_success_callback=on_task_success_callback,
    on_failure_callback=on_failure_callback,
    params={
        "pip_package_list": [
            "oss2==2.18.1",
            "pyspark==3.5.1", "ray==2.9.0", "ray[default]==2.9.0",
            "raydp==1.6.1", "scipy==1.10.1", "Shapely==1.8.5.post1",
            "pyhive[trino]==0.6.5", "loguru==0.7.2", "pytz==2023.3",
            "oss2==2.18.1", "trino==0.326.0", "pandas==2.0.3",
            "SQLAlchemy==1.4.46", "ossfs==2023.12.0", "duckdb==1.1.3"
        ],
        "task_owner":
        "some@app.com"
    },
    attach_log=True)

wait_external_dag_task.set_upstream(start_task)
ray_per_job_runner.set_upstream(wait_external_dag_task)
