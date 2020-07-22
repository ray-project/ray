import botocore
import json
import os
import logging
import time
from ray.autoscaler.aws.utils import client_cache
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

CWA_CONFIG_SSM_PARAM_NAME = "ray_cloudwatch_agent_config"


class CloudwatchHelper:
    def __init__(self, provider_config, node_ids, cluster_name):
        self.node_ids = node_ids
        self.cluster_name = cluster_name
        self.provider_config = provider_config
        region = provider_config["region"]
        self.ec2_client = client_cache("ec2", region)
        self.ssm_client = client_cache("ssm", region)
        self.cloudwatch_client = client_cache("cloudwatch", region)

    def ssm_install_cloudwatch_agent(self):
        """Install and Start CloudWatch Agent via Systems Manager (SSM)"""

        # wait for all EC2 instance checks to complete
        try:
            waiter = self.ec2_client.get_waiter("instance_status_ok")
            waiter.wait(InstanceIds=self.node_ids)
        except botocore.exceptions.WaiterError as e:
            logger.error(
                "Failed while waiting for EC2 instance checks to complete: {}".
                format(e.message))
            raise e

        # install the cloudwatch agent on each cluster node
        parameters_cwa_install = {
            "action": ["Install"],
            "name": ["AmazonCloudWatchAgent"],
            "version": ["latest"]
        }
        response = self._send_command_to_all_nodes(
            "AWS-ConfigureAWSPackage",
            parameters_cwa_install,
        )
        command_id = response["Command"]["CommandId"]
        try:
            self._ssm_command_waiter(command_id)
            logger.info(
                "SSM successfully installed cloudwatch agent on {} nodes".
                format(len(self.node_ids)))
        except botocore.exceptions.WaiterError as e:
            logger.error(
                "{}: Failed while waiting for SSM command to complete "
                "on all cluster nodes".format(e))
            raise e

        # upload cloudwatch agent config to the SSM parameter store
        cwa_config = self._load_config_file("agent")
        self._replace_all_config_variables(
            cwa_config,
            str(self.node_ids),
            self.cluster_name,
        )
        self.ssm_client.put_parameter(
            Name=CWA_CONFIG_SSM_PARAM_NAME,
            Type="String",
            Value=json.dumps(cwa_config),
            Overwrite=True,
        )

        # satisfy collectd preconditions before installing cloudwatch agent
        parameters_run_shell = {
            "commands": [
                "mkdir -p /usr/share/collectd/",
                "touch /usr/share/collectd/types.db"
            ],
        }
        self._send_command_to_all_nodes(
            "AWS-RunShellScript",
            parameters_run_shell,
        )

        # start cloudwatch agent
        parameters_start_cwa = {
            "action": ["configure"],
            "mode": ["ec2"],
            "optionalConfigurationSource": ["ssm"],
            "optionalConfigurationLocation": [CWA_CONFIG_SSM_PARAM_NAME],
            "optionalRestart": ["yes"],
        }
        self._send_command_to_all_nodes(
            "AmazonCloudWatch-ManageAgent",
            parameters_start_cwa,
        )

    def put_cloudwatch_dashboard(self):
        """put dashboard to cloudwatch console"""

        cloudwatch_config = self.provider_config["cloudwatch"]
        dashboard_config = cloudwatch_config.get("dashboard", {})
        dashboard_name = dashboard_config.get("name", self.cluster_name)
        data = self._load_config_file("dashboard")
        widgets = []
        for node_id in self.node_ids:
            for item in data:
                self._replace_all_config_variables(item, str(node_id),
                                                   self.cluster_name)
                widgets.append(item)
            response = self.cloudwatch_client.put_dashboard(
                DashboardName=dashboard_name,
                DashboardBody=json.dumps({
                    "widgets": widgets
                }))
            issue_count = len(response.get("DashboardValidationMessages", []))
            if issue_count > 0:
                for issue in response.get("DashboardValidationMessages"):
                    logging.error("Error in dashboard config: {} - {}".format(
                        issue["Message"], issue["DataPath"]))
                raise Exception(
                    "Errors in dashboard configuration: {} issues raised".
                    format(issue_count))
            else:
                logger.info("Successfully put dashboard to cloudwatch console")
        return response

    def put_cloudwatch_alarm(self):
        """ put alarms to cloudwatch console """

        data = self._load_config_file("alarm")
        for node in self.node_ids:
            for item in data:
                self._replace_all_config_variables(
                    item,
                    str(node),
                    self.cluster_name,
                )
                self.cloudwatch_client.put_metric_alarm(**item)
        logger.info("Successfully put alarms to cloudwatch console")

    def _send_command_to_all_nodes(self, document_name, parameters):
        """ send SSM command to all nodes """

        response = self.ssm_client.send_command(
            InstanceIds=self.node_ids,
            DocumentName=document_name,
            Parameters=parameters,
            MaxConcurrency=str(min(len(self.node_ids), 100)),
            MaxErrors="0")
        return response

    def _ssm_command_waiter(self, command_id):
        """ wait for SSM command to complete on all cluster nodes """

        # This waiter differs from the built-in SSM.Waiter by
        # optimistically waiting for the command invocation to
        # exist instead of failing immediately
        cloudwatch_config = self.provider_config["cloudwatch"]
        agent_retryer_config = cloudwatch_config \
            .get("agent") \
            .get("retryer", {})
        max_attempts = agent_retryer_config.get("max_attempts", 20)
        delay_seconds = agent_retryer_config.get("delay_seconds", 2)
        num_attempts = 0
        for node_id in self.node_ids:
            while True:
                num_attempts += 1
                try:
                    output_response = self.ssm_client.get_command_invocation(
                        CommandId=command_id,
                        InstanceId=node_id,
                    )
                    if output_response["Status"] == "Success":
                        logger.debug("Waiting complete, waiter matched the "
                                     "success state.")
                        break
                    if num_attempts >= max_attempts:
                        logger.error(
                            "Max attempts of get_command_invocation exceeded"
                            "on node: {}".format(node_id))
                        raise botocore.exceptions.WaiterError(
                            name="ssm_waiter",
                            reason="Max attempts exceeded",
                            last_response=output_response,
                        )
                except ClientError as e:
                    if e.response["Error"]["Code"] == "InvocationDoesNotExist":
                        logger.debug(
                            str(e) + " Maybe the command was just started," +
                            " and will take a moment to register")
                time.sleep(delay_seconds)

    def _replace_config_variables(self, string, node_id, cluster_name):
        """ replace known config variable occurrences in the input string """

        return string.replace(
            "{region}", self.provider_config["region"]). \
            replace("{instance_id}", node_id). \
            replace("{cluster_name}", cluster_name)

    def _replace_all_config_variables(self, collection, node_id, cluster_name):
        """
        Replace known config variable occurrences in the input collection.
        The input collection must be either a dict or list.
        """

        if type(collection) is dict:
            for key, value in collection.items():
                if type(value) is dict or type(value) is list:
                    collection[key] = self._replace_all_config_variables(
                        collection[key],
                        node_id,
                        cluster_name,
                    )
                elif type(value) is str:
                    collection[key] = self._replace_config_variables(
                        value,
                        node_id,
                        cluster_name,
                    )
        if type(collection) is list:
            for i in range(len(collection)):
                if type(collection[i]) is dict or type(collection[i]) is list:
                    collection[i] = self._replace_all_config_variables(
                        collection[i],
                        node_id,
                        cluster_name,
                    )
                elif type(collection[i]) is str:
                    collection[i] = self._replace_config_variables(
                        collection[i],
                        node_id,
                        cluster_name,
                    )
        return collection

    def _load_config_file(self, section_name):
        """load JSON config file"""

        cloudwatch_config = self.provider_config["cloudwatch"]
        json_config_file_section = cloudwatch_config.get(section_name, {})
        json_config_file_path = json_config_file_section.get("config", {})
        json_config_path = os.path.abspath(json_config_file_path)
        with open(json_config_path) as f:
            data = json.load(f)
        return data


def cloudwatch_config_exists(config, section_name, file_name):
    """check if cloudwatch config file exists"""

    cfg = config.get("cloudwatch", {}).get(section_name, {}).get(file_name)
    if cfg:
        assert os.path.isfile(cfg), \
            "Invalid CloudWatch Config File Path: {}".format(cfg)
    return bool(cfg)
