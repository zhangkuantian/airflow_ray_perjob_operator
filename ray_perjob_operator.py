# coding=utf-8
import time

from airflow.models import BaseOperator
from airflow.utils import timezone
from typing import Optional, Sequence

import yaml
from airflow.exceptions import AirflowSensorTimeout, AirflowException
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from kubernetes import client


class CustomKubernetesHook(KubernetesHook):
    default_conn_name = 'kubernetes_default'

    def __init__(
            self,
            conn_id: str = default_conn_name,
            client_configuration: Optional[client.Configuration] = None
    ) -> None:
        super().__init__()
        self.conn_id = conn_id
        self.client_configuration = client_configuration

    def wait_for_object_delete(self, group: str, version: str, plural: str,
                               name: str, namespace: str):
        """

        :param group:
        :param version:
        :param plural:
        :param name:
        :param namespace:
        :return:
        """
        try:
            while True:
                resp = self.get_custom_object(group, version, plural, name,
                                              namespace)
                if not resp:
                    break
                status = resp.status.phase
                self.log.info(
                    "Object: %s at namespace: %s current status: %s" %
                    (name, namespace, status))
                time.sleep(10)
        except client.rest.ApiException as e:
            raise AirflowException(
                f"Exception when calling -> wait_for_object_delete: {e}\n")

    def delete_custom_object(self, group: str, version: str, plural: str,
                             name: str, namespace: str):
        """

        :param group:
        :param version:
        :param plural:
        :param name:
        :param namespace:
        :return:
        """
        api = client.CustomObjectsApi(self.api_client)
        if namespace is None:
            namespace = self.get_namespace()
        try:
            response = api.delete_namespaced_custom_object(
                group, version, namespace, plural, name)
            return response
        except client.rest.ApiException as e:
            raise AirflowException(
                f"Exception when calling -> delete_namespaced_custom_object: {e}\n"
            )


class RayPerJobOperator(BaseOperator):
    FAILURE_STATES = ("FAILED", "UNKNOWN")
    STOP_STATUS = ("STOPPED", )
    SUCCESS_STATES = ("SUCCEEDED", )
    template_fields: Sequence[str] = ("application_file", "namespace")
    template_ext: Sequence[str] = (".yaml", ".yml", ".json")
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        application_file: str,
        attach_log: bool = False,
        namespace: Optional[str] = None,
        kubernetes_conn_id: str = "kubernetes_default",
        api_group: str = 'ray.io',
        api_version: str = 'v1',
        poke_interval: int = 60,
        timeout: int = 90 * 60,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.application_file = application_file
        self.attach_log = attach_log
        self.namespace = namespace
        self.timeout = timeout
        self.kubernetes_conn_id = kubernetes_conn_id
        self.api_group = api_group
        self.api_version = api_version
        self.poke_interval = poke_interval

    def __load_body_to_dict(self):
        try:
            body_dict = yaml.safe_load(self.application_file)
        except yaml.YAMLError as e:
            raise AirflowException(
                f"Exception when loading resource definition: {e}\n")
        return body_dict

    def _log_driver(self, hook, job_state: str, response: dict) -> None:
        if not self.attach_log:
            return
        status_info = response["status"]
        if "jobId" not in status_info:
            return
        job_pod_id = status_info["jobId"]
        namespace = response["metadata"]["namespace"]
        dashboard_url = response["status"]["dashboardURL"]
        log_method = self.log.error if job_state in self.FAILURE_STATES + self.STOP_STATUS else self.log.info
        try:
            log = ""
            from ray.job_submission import JobSubmissionClient
            job_client = JobSubmissionClient(address=f"http://{dashboard_url}")
            log += job_client.get_job_logs(job_pod_id)
            log_method(log)
        except client.rest.ApiException as e:
            self.log.warning(
                "Could not read logs for pod %s. It may have been disposed.\n"
                "Make sure timeToLiveSeconds is set on your RayJob spec.\n"
                "underlying exception: %s",
                job_pod_id,
                e,
            )

    def __poke_pod(self, hook, application_name):
        self.log.info("Poking: %s", application_name)
        response = hook.get_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural="rayjobs",
            name=application_name,
            namespace=self.namespace,
        )
        try:
            job_state = response["status"]["jobStatus"]
        except KeyError:
            return False
        if self.attach_log and job_state in self.FAILURE_STATES + self.SUCCESS_STATES + self.STOP_STATUS:
            self._log_driver(hook, job_state, response)
        if job_state in self.FAILURE_STATES:
            raise AirflowException(f"RayJob failed with state: {job_state}")
        elif job_state in self.STOP_STATUS:
            raise AirflowException(f"RayJob stopped with state: {job_state}")
        elif job_state in self.SUCCESS_STATES:
            self.log.info("RayJob ended successfully")
            return True
        else:
            self.log.info("RayJob is still in state: %s", job_state)
            return False

    def execute(self, context):
        self.log.info("Creating RayJob with file: %s" % self.application_file)
        start_time = timezone.utcnow()
        hook = CustomKubernetesHook(conn_id=self.kubernetes_conn_id)
        response = hook.create_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural="rayjobs",
            body=self.application_file,
            namespace=self.namespace,
        )
        try:
            if response and response['metadata'] and response['metadata'][
                    'name']:
                job_name = response['metadata']['name']
                task_state = self.__poke_pod(hook, job_name)
                while not task_state:
                    time.sleep(self.poke_interval)
                    if self.timeout < (timezone.utcnow() -
                                       start_time).total_seconds():
                        raise AirflowSensorTimeout(
                            f"Snap. Time is OUT. DAG id: {self.dag.dag_id} task id: {self.task_id}"
                        )
                    task_state = self.__poke_pod(hook, job_name)
            else:
                self.log.error(
                    f"Submit RayJob failed with application file: {self.application_file}"
                )
                raise AirflowException(
                    f"Submit RayJob failed with application file: {self.application_file}"
                )
            return response
        finally:
            object_resource = self.__load_body_to_dict()
            self.log.warning("delete rayjob cluster for ray job: %s",
                             object_resource['metadata']['name'])
            hook.delete_custom_object(
                group=self.api_group,
                version=self.api_version,
                plural="rayjobs",
                name=object_resource['metadata']['name'],
                namespace=self.namespace,
            )

    def on_kill(self) -> None:
        try:
            hook = CustomKubernetesHook(conn_id=self.kubernetes_conn_id)
            object_resource = self.__load_body_to_dict()
            response = hook.get_custom_object(
                group=self.api_group,
                version=self.api_version,
                plural='rayjobs',
                name=object_resource['metadata']['name'],
                namespace=self.namespace)
            if not response:
                self.log.warning(
                    "custom object with plural: %s name: %s not found at namespace: %s!"
                    % ("RayJob", object_resource['metadata']['name'],
                       self.namespace))
                return
            if self.attach_log:
                self._log_driver(hook, self.FAILURE_STATES, response)
            hook.delete_custom_object(
                group=self.api_group,
                version=self.api_version,
                plural="rayjobs",
                name=object_resource['metadata']['name'],
                namespace=self.namespace,
            )
            self.log.info(
                "Delete custom object with plural: %s name: %s succeed!" %
                ("RayJob", object_resource['metadata']['name']))
        except Exception as e:
            self.log.error(
                "call on_kill method failed! with application file: %s Error message: %s"
                % (self.application_file, e))


def create_ray_per_job_k8s_operator(task_id,
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
