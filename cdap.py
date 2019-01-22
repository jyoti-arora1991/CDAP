import logging

from enum import Enum

from api_request import ApiRequest
from api_request_specification import ApiRequestSpecification
from request_type import RequestType


class Cdap(object):
    def __init__(self, node_ip, rest_port,ui_port, username, password, kerberized = None, namespace_id="default"):
        """

        :param namespace_id: CDAP's namespace ID.
        :type namespace_id: str
        """
        self._logger = logging.getLogger(__name__)
        self.username = username
        self.password = password
        self.node_ip = node_ip
        self.ui_port = ui_port
        self.rest_port = rest_port
        self.kerberized = kerberized
        self.ui_base_url = "http://%s:%s" % (node_ip, ui_port)
        self.rest_base_url = "http://%s:%s" % (node_ip, rest_port)
        self.rest_url = "%s/v3/namespaces/%s" % (self.rest_base_url, namespace_id)
        self.api_request = ApiRequest()
        self.verify = True
        self.access_token = self.get_access_token(self.username,
                                                  self.password) if kerberized is True else None

    def get_access_token(self, username, password):
        """Get the bearer type access token generated as per the user credentials `username` and `password` to access
        Kerberos authenticated CDAP.

        :param username: UI username to be used.
        :type username: str
        :param password: UI password to be used
        :type password: str
        :return: A bearer token.
        :rtype: str
        """
        self.verify = False
        self.ui_base_url = "https://%s:%s" % (self.node_ip, self.ui_port)
        self.rest_base_url = "https://%s:%s" % (self.node_ip, self.rest_port)
        url = "%s/login" % self.ui_base_url
        payload = {"username": username, "password": password}
        api_request_specification = ApiRequestSpecification(RequestType.POST, url, verify=self.verify, json=payload)
        return self.api_request.execute_request(api_request_specification).json()["access_token"]

    def execute_request(self, api_request_specification):
        """
        Execute the api request as per the given `api_request_specification`. For kerberos authenticated CDAP, a bearer
        type access token will passed along with the api request for user authentication.

        :type api_request_specification: :class:`nimble.core.api.api_request_specification.ApiRequestSpecification`
        :rtype: :class:`requests.models.Response`
        """
        if self.kerberized is True:
            api_request_specification.set_bearer_token(self.access_token)
        return self.api_request.execute_request(api_request_specification)

    def apps(self, artifact_name=None, artifact_version=None):
        """Get all CDAP apps.

        Refer to the official API documentation `here <https://docs.cask.co/cdap/4.3.4/en/reference-manual/http-restful-api/lifecycle.html#deployed-applications>`_.

        :param artifact_name: Name of the artifact on which the results need to be filtered.
        :type artifact_name: str
        :param artifact_version: Version of the artifact on which the results need to be filtered.
        :type artifact_version: str
        :return: Response containing json as pipeline details.
        :rtype: :class:`requests.models.Response`
        """
        url = "%s/apps" % self.rest_url
        query_params = {}
        if artifact_name:
            query_params["artifactName"] = artifact_name
        if artifact_version:
            query_params["artifactVersion"] = artifact_version
        api_request_specification = ApiRequestSpecification(RequestType.GET, url, query_params=query_params,
                                                            verify=self.verify)
        return self.execute_request(api_request_specification)

    def get_all_pipelines(self, artifact_name=None, artifact_version=None):
        """Get all CDAP pipeline names.

        :param artifact_name: Name of the artifact on which the results need to be filtered.
        :type artifact_name: str
        :param artifact_version: Version of the artifact on which the results need to be filtered.
        :type artifact_version: str
        :return: List containing all pipeline names.
        :rtype: list
        """
        pipelines = []
        for pipeline in self.apps(artifact_name=artifact_name, artifact_version=artifact_version).json():
            pipelines.append(pipeline["name"])
        return pipelines

    def create_pipeline(self, pipeline_name, overwrite_pipeline, input_json):
        """Create a CDAP pipeline.

        Refer to the official API documentation `here <https://docs.cask.co/cdap/4.3.3/en/reference-manual/http-restful-api/lifecycle.html#create-an-application>`_.

        :param pipeline_name: Name of the pipeline to be created.
        :type pipeline_name: str
        :param input_json: json for creating pipeline.
        :type input_json: dict
        :param overwrite_pipeline: Overwrite existing pipeline or not. Defaults to `False`.
        :type overwrite_pipeline: bool
        :return: Return `False` if pipeline exists else the query response containing text as "Deploy Complete".
        :rtype: bool or :class:`requests.models.Response`
        """
        if overwrite_pipeline is False and self.get_pipeline_info(pipeline_name).status_code == 200:
            self._logger.warn("Pipeline %s already exists" % pipeline_name)
            return False
        rest_url = "%s/apps/%s" % (self.rest_url, pipeline_name)
        api_request_specification = ApiRequestSpecification(RequestType.PUT, rest_url, json=input_json,
                                                            verify=self.verify)
        return self.execute_request(api_request_specification)

    def get_pipeline_info(self, pipeline_name):
        """Get detailed information of a pipeline that has been deployed.

        Refer to the official API documentation `here <https://docs.cask.co/cdap/4.3.3/en/reference-manual/http-restful-api/lifecycle.html#details-of-a-deployed-application`_.

        :param pipeline_name: Name of the pipeline user wants to get.
        :type pipeline_name: str
        :return: Response containing json as pipeline details.
        :rtype: :class:`requests.models.Response`
        """
        rest_url = "%s/apps/%s" % (self.rest_url, pipeline_name)
        api_request_specification = ApiRequestSpecification(RequestType.GET, rest_url, verify=self.verify)
        return self.execute_request(api_request_specification)

    def get_programs(self, pipeline_name):
        """Get flow and workflow of deployed pipeline.

        Refer to the official API documentation `here <https://docs.cask.co/cdap/4.3.3/en/reference-manual/http-restful-api/lifecycle.html#details-of-a-program>`_.

        :param pipeline_name: Name of the pipeline for which user wants to get worklow details.
        :type pipeline_name: str
        :return: Program list for pipeline.
        :rtype: list
        """
        return self.get_pipeline_info(pipeline_name).json()["programs"]

    def get_workflow(self, pipeline_name):
        """Get workflow of deployed pipeline.

        :param pipeline_name: Name of the pipeline from which main flow will be extracted.
        :type pipeline_name: str
        :return: Return workflow if program type corresponding to pipeline `pipeline_name` is `Workflow` or `Spark` else
            `False`.
        :rtype: dict
        """
        program_list = self.get_programs(pipeline_name)
        return Workflows.get_cdap_workflow(program_list)

    def start_pipeline(self, pipeline_name):
        """To start flow, MapReduce and Spark programs of deployed pipeline.

        Refer to the official API documentation 'here <https://docs.cask.co/cdap/4.3.3/en/reference-manual/http-restful-api/lifecycle.html#start-a-program>`_.

        :param pipeline_name: Name of the pipeline to be run.
        :type pipeline_name: str
        :return: Response containing json as JSON array.
        :rtype: :class:`requests.models.Response`
        """
        rest_url = "%s/start" % self.rest_url
        workflow = self.get_workflow(pipeline_name)
        workflow_json = [{"appId": "%s" % workflow["app"], "programType": "%s" % workflow["type"],
                          "programId": "%s" % workflow["name"]}]
        api_request_specification = ApiRequestSpecification(RequestType.POST, rest_url, json=workflow_json,
                                                            verify=self.verify)
        return self.execute_request(api_request_specification)

    def get_workflow_executions(self, pipeline_name):
        """

        Get executions of a workflow that were executed in the given pipeline. Refer to the official API documentation
        'here <https://docs.cask.co/cdap/4.0.1/en/reference-manual/http-restful-api/workflow.html#workflow-state>'_.

        :param pipeline_name: Name of the pipeline where workflow executions were executed.
        :type pipeline_name: str
        :return: Return json containing list of dictionaries where each dictionary specifies workflow execution details
            or an empty list in case the pipeline is deployed only and not executed.
        :rtype: :class:`requests.models.Response`
        """
        workflow = self.get_workflow(pipeline_name)
        workflow_id = workflow["name"]
        workflow_type = Workflows.get_api_workflow_name(workflow["type"])
        rest_url = "%s/apps/%s/%s/%s/runs" % (self.rest_url, pipeline_name, workflow_type, workflow_id)
        api_request_specification = ApiRequestSpecification(RequestType.GET, rest_url, verify=self.verify)
        return self.execute_request(api_request_specification)

    def get_workflow_execution_by_id(self, pipeline_name, execution_id):
        """

        Get the execution of a workflow that was executed with the given `execution_id` in the given pipeline.
        Refer to the official API documentation 'here <https://docs.cask.co/cdap/4.0.1/en/reference-manual/http-restful-api/workflow.html#workflow-state>'_.

        :param pipeline_name: Name of the pipeline where the workflow was executed with the given `execution_id`.
        :type pipeline_name: str
        :param execution_id: Execution id of a workflow.
        :type execution_id: str
        :return: Return json containing dictionary where dictionary specifies workflow execution details.
        :rtype: :class:`requests.models.Response`
        """
        workflow = self.get_workflow(pipeline_name)
        workflow_id = workflow["name"]
        workflow_type = Workflows.get_api_workflow_name(workflow["type"])
        rest_url = "%s/apps/%s/%s/%s/runs/%s" % (self.rest_url, pipeline_name, workflow_type, workflow_id, execution_id)
        api_request_specification = ApiRequestSpecification(RequestType.GET, rest_url, verify=self.verify)
        return self.execute_request(api_request_specification)

    def get_last_executed_workflow_id(self, pipeline_name):
        """Get id of the last executed workflow in the given pipeline.

        :param pipeline_name: Name of the pipeline where workflow was executed.
        :type pipeline_name: str
        :return: Return the last workflow execution id or a `""` if pipeline in never executed.
        :rtype: str
        """
        execution_list = self.get_workflow_executions(pipeline_name).json()
        if len(execution_list) == 0:
            return ""
        index_of_max_start_time = \
            sorted([(item["starting"], index) for index, item in enumerate(execution_list)], reverse=True)[0][1]
        return str(execution_list[index_of_max_start_time]["runid"])

    def get_last_executed_workflow_status(self, pipeline_name):
        """Get the status of the last executed workflow in the given pipeline.

        :param pipeline_name: Name of the pipeline where workflow was executed.
        :type pipeline_name: str
        :return: Return status as FAILED/KILLED/COMPLETED/NOT_EXECUTED.
        :rtype: str
        """
        run_id = self.get_last_executed_workflow_id(pipeline_name)
        return self._update_status(pipeline_name, run_id)

    def get_workflow_execution_status_by_id(self, pipeline_name, execution_id):
        """Get the status of the workflow that was executed with the given `execution_id`.

        :param pipeline_name: Name of the pipeline where workflow was executed.
        :type pipeline_name: str
        :param execution_id: Execution id of a workflow.
        :type execution_id: str
        :return: Return status as FAILED/KILLED/COMPLETED/NOT_EXECUTED.
        :rtype: str
        """
        return self._update_status(pipeline_name, execution_id)

    def query_dataset(self, query):
        """Submit SQL-like queries over datasets. Queries are processed asynchronously.

        Refer to the official API documentation 'here <https://docs.cask.co/cdap/4.3.4/en/reference-manual/http-restful-api/query.html#submitting-a-query>'_.

        :param query: Query to be executed on the dataset.
        :type query: str
        :return: API response which will contain the query handle which can be later used to fetch the status and query
            results.
        :rtype: :class:`requests.models.Response`
        """
        url = "%s/data/explore/queries" % self.rest_url
        api_request_specification = ApiRequestSpecification(RequestType.POST, url, json={"query": query},
                                                            verify=self.verify)
        return self.execute_request(api_request_specification)

    def get_query_status(self, query_handle):
        """Get the status of the SQL query run over the CDAP dataset.

        Refer to the official API documentation 'here <https://docs.cask.co/cdap/4.3.4/en/reference-manual/http-restful-api/query.html#status-of-a-query>'_.

        :param query_handle: Handle obtained when the query was submitted.
        :type query_handle: str
        :return: API response containing the status of the query. Status can be one of the following: `INITIALIZED`,
            `RUNNING`, `FINISHED`, `CANCELED`, `CLOSED`, `ERROR`, `UNKNOWN` and `PENDING`.
        :rtype: :class:`requests.models.Response`
        """
        url = "%s/v3/data/explore/queries/%s/status" % (self.rest_base_url, query_handle)
        api_request_specification = ApiRequestSpecification(RequestType.GET, url, verify=self.verify)
        return self.execute_request(api_request_specification)

    def download_query_results(self, query_handle):
        """Download the results of the query on the CDAP dataset in a CSV format.

        Refer to the official API documentation 'here <https://docs.cask.co/cdap/4.3.4/en/reference-manual/http-restful-api/query.html#download-query-results>'_.

        :param query_handle: Handle obtained when the query was submitted.
        :type query_handle: str
        :return: API response containing the results of the query in CSV format.
        :rtype: :class:`requests.models.Response`
        """
        url = "%s/v3/data/explore/queries/%s/download" % (self.rest_base_url, query_handle)
        api_request_specification = ApiRequestSpecification(RequestType.POST, url, verify=self.verify)
        return self.execute_request(api_request_specification)

    def view_query_result(self, query_handle):
        """Retrieve the results of the query on the CDAP dataset.

        Refer to the official API documentation 'here <https://docs.cask.co/cdap/4.3.4/en/reference-manual/http-restful-api/query.html#retrieving-query-results>'_.

        :param query_handle: Handle obtained when the query was submitted.
        :type query_handle: str
        :return: API response containing the results of the query in JSON format.
        :rtype: :class:`requests.models.Response`
        """
        url = "%s/v3/data/explore/queries/%s/next" % (self.rest_base_url, query_handle)
        api_request_specification = ApiRequestSpecification(RequestType.POST, url, verify=self.verify)
        return self.execute_request(api_request_specification)

    def schema(self, query_handle):
        """Retrieve the schema of the query results of the CDAP dataset.

        Refer to the official API documentation 'here <https://docs.cask.co/cdap/4.3.4/en/reference-manual/http-restful-api/query.html#obtaining-the-result-schema>'_.

        :param query_handle: Handle obtained when the query was submitted.
        :type query_handle: str
        :return: API response containing the results of the query in JSON format.
        :rtype: :class:`requests.models.Response`
        """
        url = "%s/v3/data/explore/queries/%s/schema" % (self.rest_base_url, query_handle)
        api_request_specification = ApiRequestSpecification(RequestType.GET, url, verify=self.verify)
        return self.execute_request(api_request_specification)

    def is_streaming_pipeline(self, pipeline_name):
        """Return `True` if the pipeline is streaming, else `False`.

        :param pipeline_name: Name of the pipeline `pipeline_name`.
        :type pipeline_name: str
        :return: Return `True` if the pipeline is streaming, else `False`.
        :rtype: bool
        """
        for type_ in self.get_pipeline_info(pipeline_name).json()["plugins"]:
            if type_["type"] == "streamingsource":
                return True
        return False

    def stop_pipeline(self, pipeline_name):
        """Stop running executions for the given pipeline `pipeline_name`.

        :param pipeline_name: Name of the pipeline.
        :type pipeline_name: str
        :rtype: :class:`requests.models.Response`
        """
        execution_id = self.get_last_executed_workflow_id(pipeline_name)
        workflow = self.get_workflow(pipeline_name)
        workflow_id = workflow["name"]
        workflow_type = Workflows.get_api_workflow_name(workflow["type"])
        rest_url = "%s/apps/%s/%s/%s/runs/%s/stop" % (
            self.rest_url, pipeline_name, workflow_type, workflow_id, execution_id)
        api_request_specification = ApiRequestSpecification(RequestType.POST, rest_url, verify=self.verify)
        return self.execute_request(api_request_specification)

    def stop_all_pipelines(self):
        """Stop running executions for all the pipelines."""
        for pipeline in self.get_all_pipelines():
            try:
                self.stop_pipeline(pipeline)
            except Exception:
                self._logger.exception("Cannot stop pipeline %s" % pipeline)

    def delete_pipeline(self, pipeline_name):
        """Delete given pipeline `pipeline_name`.

        :param pipeline_name: Name of the pipeline which is to be deleted.
        :type pipeline_name: str
        :rtype: :class:`requests.models.Response`
        """
        rest_url = "%s/apps/%s" % (self.rest_url, pipeline_name)
        api_request_specification = ApiRequestSpecification(RequestType.DELETE, rest_url, verify=self.verify)
        return self.execute_request(api_request_specification)

    def delete_all_pipelines(self, artifact_name=None, artifact_version=None):
        """Delete all the pipelines on the system.

        :param artifact_name: Name of the artifact on which the results need to be filtered.
        :type artifact_name: str
        :param artifact_version: Version of the artifact on which the results need to be filtered.
        :type artifact_version: str
        """
        for pipeline in self.get_all_pipelines(artifact_name=artifact_name, artifact_version=artifact_version):
            self.delete_pipeline(pipeline)

    def _update_status(self, pipeline_name, execution_id):
        """Update status of the workflow to `NOT_EXECUTED` in case the status is returned as empty list.

        :param pipeline_name: Name of the pipeline where workflow was executed.
        :type pipeline_name: str
        :param execution_id: Execution id of a workflow.
        :type execution_id: str
        :return: Return status as FAILED/KILLED/COMPLETED/NOT_EXECUTED.
        :rtype: str
        """
        if execution_id == "":
            return "NOT_EXECUTED"
        status = self.get_workflow_execution_by_id(pipeline_name, execution_id).json()["status"]
        return str(status)


class Workflows(Enum):
    """Enumeration of the CDAP workflows."""
    WORKFLOW = "workflows"
    SPARK = "spark"

    @staticmethod
    def get_api_workflow_name(workflow):
        """Return workflow name correspoding to `workflow` or `spark` and the workflow name will be used in the CDAP Api
        ."""
        return Workflows[workflow.upper()].value

    @staticmethod
    def get_cdap_workflow(program_list):
        """Return workflow correspoding to `workflow` or `spark`.

        :param program_list: List of the CDAP workflows.
        :type program_list: list
        :return: Return workflow correspoding to `workflow` or `spark`.
        :rtype: dict
        """
        spark_program = None
        for program in program_list:
            program_type = program["type"].upper()
            if program_type == Workflows.WORKFLOW.name:
                return program
            elif program_type == Workflows.SPARK.name:
                spark_program = program
        return spark_program
