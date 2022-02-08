import botocore
import copy
import json
import os
import logging
import time
import hashlib
from typing import Any, Dict, List, Union, Tuple
from ray.autoscaler._private.aws.utils import client_cache, resource_cache
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, NODE_KIND_HEAD, TAG_RAY_NODE_KIND

logger = logging.getLogger(__name__)

RAY = "ray-autoscaler"
CLOUDWATCH_RAY_INSTANCE_PROFILE = RAY + "-cloudwatch-v1"
CLOUDWATCH_RAY_IAM_ROLE = RAY + "-cloudwatch-v1"
CLOUDWATCH_AGENT_INSTALLED_AMI_TAG = "T6Iq2faj"
CLOUDWATCH_AGENT_INSTALLED_TAG = "cloudwatch-agent-installed"
CLOUDWATCH_CONFIG_HASH_TAG_BASE = "cloudwatch-config-hash"


class CloudwatchHelper:
    def __init__(
        self, provider_config: Dict[str, Any], node_id: str, cluster_name: str
    ) -> None:
        self.node_id = node_id
        self.cluster_name = cluster_name
        self.provider_config = provider_config
        region = provider_config["region"]
        self.ec2_resource = resource_cache("ec2", region)
        self.ec2_client = self.ec2_resource.meta.client
        self.ssm_client = client_cache("ssm", region)
        cloudwatch_resource = resource_cache("cloudwatch", region)
        self.cloudwatch_client = cloudwatch_resource.meta.client

    def update_from_config(self, is_head_node: bool) -> None:
        """Discovers and applies CloudWatch config updates as required.

        Args:
            is_head_node: whether this node is the head node.
        """
        if CloudwatchHelper.cloudwatch_config_exists(self.provider_config, "agent"):
            self._update_cloudwatch_config(is_head_node, "agent")
        if CloudwatchHelper.cloudwatch_config_exists(self.provider_config, "dashboard"):
            self._update_cloudwatch_config(is_head_node, "dashboard")
        if CloudwatchHelper.cloudwatch_config_exists(self.provider_config, "alarm"):
            self._update_cloudwatch_config(is_head_node, "alarm")

    def _ec2_health_check_waiter(self, node_id: str) -> None:
        # wait for all EC2 instance checks to complete
        try:
            logger.info(
                "Waiting for EC2 instance health checks to complete before "
                "configuring Unified Cloudwatch Agent. This may take a few "
                "minutes..."
            )
            waiter = self.ec2_client.get_waiter("instance_status_ok")
            waiter.wait(InstanceIds=[node_id])
        except botocore.exceptions.WaiterError as e:
            logger.error(
                "Failed while waiting for EC2 instance checks to complete: {}".format(
                    e.message
                )
            )
            raise e

    def _update_cloudwatch_config(self, is_head_node: bool, config_type: str) -> None:
        """Update remote CloudWatch configs at Parameter Store,
        update hash tag value on node and perform associated operations
        at CloudWatch console if local CloudWatch configs change.

        Args:
            is_head_node: whether this node is the head node.
            config_type: CloudWatch config file type.
        """
        cwa_installed = self._setup_cwa()
        param_name = self._get_ssm_param_name(config_type)
        if cwa_installed:
            if is_head_node:
                cw_config_ssm = self._set_cloudwatch_ssm_config_param(
                    param_name, config_type
                )
                cur_cw_config_hash = self._sha1_hash_file(config_type)
                ssm_cw_config_hash = self._sha1_hash_json(cw_config_ssm)
                # check if user updated Unified Cloudwatch Agent config file.
                # if so, perform corresponding actions.
                if cur_cw_config_hash != ssm_cw_config_hash:
                    logger.info(
                        "Cloudwatch {} config file has changed.".format(config_type)
                    )
                    self._upload_config_to_ssm_and_set_hash_tag(config_type)
                    if config_type == "agent":
                        self._restart_cloudwatch_agent()
                    elif config_type == "dashboard":
                        self._put_cloudwatch_dashboard()
                    elif config_type == "alarm":
                        self._put_cloudwatch_alarm()
            else:
                head_node_hash = self._get_head_node_config_hash(config_type)
                cur_node_hash = self._get_cur_node_config_hash(config_type)
                if head_node_hash != cur_node_hash:
                    logger.info(
                        "Cloudwatch {} config file has changed.".format(config_type)
                    )
                    if config_type == "agent":
                        self._restart_cloudwatch_agent()
                    if config_type == "alarm":
                        self._put_cloudwatch_alarm()
                    self._update_cloudwatch_hash_tag_value(
                        self.node_id, head_node_hash, config_type
                    )

    def _put_cloudwatch_dashboard(self) -> Dict[str, Any]:
        """put dashboard to cloudwatch console"""

        cloudwatch_config = self.provider_config["cloudwatch"]
        dashboard_config = cloudwatch_config.get("dashboard", {})
        dashboard_name_cluster = dashboard_config.get("name", self.cluster_name)
        dashboard_name = self.cluster_name + "-" + dashboard_name_cluster

        widgets = self._replace_dashboard_config_variables()

        response = self.cloudwatch_client.put_dashboard(
            DashboardName=dashboard_name, DashboardBody=json.dumps({"widgets": widgets})
        )
        issue_count = len(response.get("DashboardValidationMessages", []))
        if issue_count > 0:
            for issue in response.get("DashboardValidationMessages"):
                logging.error(
                    "Error in dashboard config: {} - {}".format(
                        issue["Message"], issue["DataPath"]
                    )
                )
            raise Exception(
                "Errors in dashboard configuration: {} issues raised".format(
                    issue_count
                )
            )
        else:
            logger.info("Successfully put dashboard to CloudWatch console")
        return response

    def _put_cloudwatch_alarm(self) -> None:
        """put CloudWatch metric alarms read from config"""
        param_name = self._get_ssm_param_name("alarm")
        data = json.loads(self._get_ssm_param(param_name))
        for item in data:
            item_out = copy.deepcopy(item)
            self._replace_all_config_variables(
                item_out,
                self.node_id,
                self.cluster_name,
                self.provider_config["region"],
            )
            self.cloudwatch_client.put_metric_alarm(**item_out)
        logger.info("Successfully put alarms to CloudWatch console")

    def _send_command_to_node(
        self, document_name: str, parameters: List[str], node_id: str
    ) -> Dict[str, Any]:
        """send SSM command to the given nodes"""
        logger.debug(
            "Sending SSM command to {} node(s). Document name: {}. "
            "Parameters: {}.".format(node_id, document_name, parameters)
        )
        response = self.ssm_client.send_command(
            InstanceIds=[node_id],
            DocumentName=document_name,
            Parameters=parameters,
            MaxConcurrency="1",
            MaxErrors="0",
        )
        return response

    def _ssm_command_waiter(
        self,
        document_name: str,
        parameters: List[str],
        node_id: str,
        retry_failed: bool = True,
    ) -> bool:
        """wait for SSM command to complete on all cluster nodes"""

        # This waiter differs from the built-in SSM.Waiter by
        # optimistically waiting for the command invocation to
        # exist instead of failing immediately, and by resubmitting
        # any failed command until all retry attempts are exhausted
        # by default.
        response = self._send_command_to_node(document_name, parameters, node_id)
        command_id = response["Command"]["CommandId"]

        cloudwatch_config = self.provider_config["cloudwatch"]
        agent_retryer_config = cloudwatch_config.get("agent").get("retryer", {})
        max_attempts = agent_retryer_config.get("max_attempts", 120)
        delay_seconds = agent_retryer_config.get("delay_seconds", 30)
        num_attempts = 0
        cmd_invocation_res = {}
        while True:
            num_attempts += 1
            logger.debug(
                "Listing SSM command ID {} invocations on node {}".format(
                    command_id, node_id
                )
            )
            response = self.ssm_client.list_command_invocations(
                CommandId=command_id,
                InstanceId=node_id,
            )
            cmd_invocations = response["CommandInvocations"]
            if not cmd_invocations:
                logger.debug(
                    "SSM Command ID {} invocation does not exist. If "
                    "the command was just started, it may take a "
                    "few seconds to register.".format(command_id)
                )
            else:
                if len(cmd_invocations) > 1:
                    logger.warning(
                        "Expected to find 1 SSM command invocation with "
                        "ID {} on node {} but found {}: {}".format(
                            command_id,
                            node_id,
                            len(cmd_invocations),
                            cmd_invocations,
                        )
                    )
                cmd_invocation = cmd_invocations[0]
                if cmd_invocation["Status"] == "Success":
                    logger.debug(
                        "SSM Command ID {} completed successfully.".format(command_id)
                    )
                    cmd_invocation_res[node_id] = True
                    break
                if num_attempts >= max_attempts:
                    logger.error(
                        "Max attempts for command {} exceeded on node {}".format(
                            command_id, node_id
                        )
                    )
                    raise botocore.exceptions.WaiterError(
                        name="ssm_waiter",
                        reason="Max attempts exceeded",
                        last_response=cmd_invocation,
                    )
                if cmd_invocation["Status"] == "Failed":
                    logger.debug(f"SSM Command ID {command_id} failed.")
                    if retry_failed:
                        logger.debug(f"Retrying in {delay_seconds} seconds.")
                        response = self._send_command_to_node(
                            document_name, parameters, node_id
                        )
                        command_id = response["Command"]["CommandId"]
                        logger.debug(
                            "Sent SSM command ID {} to node {}".format(
                                command_id, node_id
                            )
                        )
                    else:
                        logger.debug(f"Ignoring Command ID {command_id} failure.")
                        cmd_invocation_res[node_id] = False
                        break
            time.sleep(delay_seconds)

        return cmd_invocation_res

    def _replace_config_variables(
        self, string: str, node_id: str, cluster_name: str, region: str
    ) -> str:
        """
        replace known config variable occurrences in the input string
        does not replace variables with undefined or empty strings
        """

        if node_id:
            string = string.replace("{instance_id}", node_id)
        if cluster_name:
            string = string.replace("{cluster_name}", cluster_name)
        if region:
            string = string.replace("{region}", region)
        return string

    def _replace_all_config_variables(
        self,
        collection: Union[dict, list],
        node_id: str,
        cluster_name: str,
        region: str,
    ) -> Tuple[(Union[dict, list], int)]:
        """
        Replace known config variable occurrences in the input collection.

        The input collection must be either a dict or list.
        Returns a tuple consisting of the output collection and the number of
        modified strings in the collection (which is not necessarily equal to
        the number of variables replaced).
        """
        for key in collection:
            if type(collection) is dict:
                value = collection.get(key)
                index_key = key
            elif type(collection) is list:
                value = key
                index_key = collection.index(key)
            if type(value) is str:
                collection[index_key] = self._replace_config_variables(
                    value, node_id, cluster_name, region
                )
            elif type(value) is dict or type(value) is list:
                collection[index_key] = self._replace_all_config_variables(
                    value, node_id, cluster_name, region
                )
        return collection

    def _load_config_file(self, config_type: str) -> Dict[str, Any]:
        """load JSON config file"""
        cloudwatch_config = self.provider_config["cloudwatch"]
        json_config_file_section = cloudwatch_config.get(config_type, {})
        json_config_file_path = json_config_file_section.get("config", {})
        json_config_path = os.path.abspath(json_config_file_path)
        with open(json_config_path) as f:
            data = json.load(f)
        return data

    def _set_cloudwatch_ssm_config_param(
        self, parameter_name: str, config_type: str
    ) -> str:
        """
        get cloudwatch config for the given param and config type from SSM
        if it exists, put it in the SSM param store if not
        """
        try:
            parameter_value = self._get_ssm_param(parameter_name)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "ParameterNotFound":
                logger.info(
                    "Cloudwatch {} config file is not found "
                    "at SSM parameter store. "
                    "Checking for Unified CloudWatch Agent installation".format(
                        config_type
                    )
                )
                return self._get_default_empty_config_file_hash()
            else:
                logger.info(
                    "Failed to fetch CloudWatch {} config from SSM "
                    "parameter store.".format(config_type)
                )
                logger.error(e)
                raise e
        return parameter_value

    def _get_default_empty_config_file_hash(self):
        default_cw_config = "{}"
        parameter_value = self._sha1_hash_json(default_cw_config)
        return parameter_value

    def _get_ssm_param(self, parameter_name: str) -> str:
        """
        get the SSM parameter value associated with the given parameter name
        """
        response = self.ssm_client.get_parameter(Name=parameter_name)
        logger.info("Successfully fetch ssm parameter: {}".format(parameter_name))
        res = response.get("Parameter", {})
        cwa_parameter = res.get("Value", {})
        return cwa_parameter

    def _sha1_hash_json(self, value: str) -> str:
        """calculate the json string sha1 hash"""
        hash = hashlib.new("sha1")
        binary_value = value.encode("ascii")
        hash.update(binary_value)
        sha1_res = hash.hexdigest()
        return sha1_res

    def _sha1_hash_file(self, config_type: str) -> str:
        """calculate the config file sha1 hash"""
        if config_type == "agent":
            config = self._replace_cwa_config_variables()
        if config_type == "dashboard":
            config = self._replace_dashboard_config_variables()
        if config_type == "alarm":
            config = self._load_config_file("alarm")
        value = json.dumps(config)
        sha1_res = self._sha1_hash_json(value)
        return sha1_res

    def _upload_config_to_ssm_and_set_hash_tag(self, config_type: str):
        if config_type == "agent":
            data = self._replace_cwa_config_variables()
        if config_type == "dashboard":
            data = self._replace_dashboard_config_variables()
        if config_type == "alarm":
            data = self._load_config_file("alarm")
        sha1_hash_value = self._sha1_hash_file(config_type)
        self._upload_config_to_ssm(data, config_type)
        self._update_cloudwatch_hash_tag_value(
            self.node_id, sha1_hash_value, config_type
        )

    def _add_cwa_installed_tag(self, node_id: str) -> None:
        self.ec2_client.create_tags(
            Resources=[node_id],
            Tags=[{"Key": CLOUDWATCH_AGENT_INSTALLED_TAG, "Value": "True"}],
        )
        logger.info(
            "Successfully add Unified Cloudwatch Agent installed "
            "tag on {}".format(node_id)
        )

    def _update_cloudwatch_hash_tag_value(
        self, node_id: str, sha1_hash_value: str, config_type: str
    ):
        hash_key_value = "-".join([CLOUDWATCH_CONFIG_HASH_TAG_BASE, config_type])
        self.ec2_client.create_tags(
            Resources=[node_id],
            Tags=[{"Key": hash_key_value, "Value": sha1_hash_value}],
        )
        logger.info(
            "Successfully update cloudwatch {} hash tag on {}".format(
                config_type, node_id
            )
        )

    def _get_ssm_param_name(self, config_type: str) -> str:
        """return the parameter name for cloudwatch configs"""
        ssm_config_param_name = "AmazonCloudWatch-" + "ray_{}_config_{}".format(
            config_type, self.cluster_name
        )
        return ssm_config_param_name

    def _put_ssm_param(self, parameter: Dict[str, Any], parameter_name: str) -> None:
        """upload cloudwatch config to the SSM parameter store"""
        self.ssm_client.put_parameter(
            Name=parameter_name,
            Type="String",
            Value=json.dumps(parameter),
            Overwrite=True,
            Tier="Intelligent-Tiering",
        )

    def _upload_config_to_ssm(self, param: Dict[str, Any], config_type: str):
        param_name = self._get_ssm_param_name(config_type)
        self._put_ssm_param(param, param_name)

    def _replace_cwa_config_variables(self) -> Dict[str, Any]:
        """
        replace {instance_id}, {region}, {cluster_name}
        variable occurrences in Unified Cloudwatch Agent config file
        """
        cwa_config = self._load_config_file("agent")
        self._replace_all_config_variables(
            cwa_config,
            self.node_id,
            self.cluster_name,
            self.provider_config["region"],
        )
        return cwa_config

    def _replace_dashboard_config_variables(self) -> List[Dict[str, Any]]:
        """
        replace known variable occurrences in CloudWatch Dashboard config file
        """
        data = self._load_config_file("dashboard")
        widgets = []
        for item in data:
            item_out = self._replace_all_config_variables(
                item,
                self.node_id,
                self.cluster_name,
                self.provider_config["region"],
            )
            item_out = copy.deepcopy(item)
            widgets.append(item_out)
        return widgets

    def _replace_alarm_config_variables(self) -> List[Dict[str, Any]]:
        """
        replace {instance_id}, {region}, {cluster_name}
        variable occurrences in cloudwatch alarm config file
        """
        data = self._load_config_file("alarm")
        param_data = []
        for item in data:
            item_out = copy.deepcopy(item)
            self._replace_all_config_variables(
                item_out,
                self.node_id,
                self.cluster_name,
                self.provider_config["region"],
            )
            param_data.append(item_out)
        return param_data

    def _restart_cloudwatch_agent(self) -> None:
        """restart Unified Cloudwatch Agent"""
        cwa_param_name = self._get_ssm_param_name("agent")
        logger.info(
            "Restarting Unified Cloudwatch Agent package on {} node(s).".format(
                (self.node_id)
            )
        )
        self._stop_cloudwatch_agent()
        self._start_cloudwatch_agent(cwa_param_name)

    def _stop_cloudwatch_agent(self) -> None:
        """stop Unified CloudWatch Agent"""
        logger.info(
            "Stopping Unified CloudWatch Agent package on node {}.".format(self.node_id)
        )
        parameters_stop_cwa = {
            "action": ["stop"],
            "mode": ["ec2"],
        }
        # don't retry failed stop commands
        # (there's not always an agent to stop)
        self._ssm_command_waiter(
            "AmazonCloudWatch-ManageAgent",
            parameters_stop_cwa,
            self.node_id,
            False,
        )
        logger.info("Unified CloudWatch Agent stopped on node {}.".format(self.node_id))

    def _start_cloudwatch_agent(self, cwa_param_name: str) -> None:
        """start Unified CloudWatch Agent"""
        logger.info(
            "Starting Unified CloudWatch Agent package on node {}.".format(self.node_id)
        )
        parameters_start_cwa = {
            "action": ["configure"],
            "mode": ["ec2"],
            "optionalConfigurationSource": ["ssm"],
            "optionalConfigurationLocation": [cwa_param_name],
            "optionalRestart": ["yes"],
        }
        self._ssm_command_waiter(
            "AmazonCloudWatch-ManageAgent", parameters_start_cwa, self.node_id
        )
        logger.info(
            "Unified CloudWatch Agent started successfully on node {}.".format(
                self.node_id
            )
        )

    def _setup_cwa(self) -> bool:
        cwa_installed = self._check_cwa_installed_ec2_tag()
        if cwa_installed == "False":
            res_cwa_installed = self._ensure_cwa_installed_ssm(self.node_id)
            return res_cwa_installed
        else:
            return True

    def _get_head_node_config_hash(self, config_type: str) -> str:
        hash_key_value = "-".join([CLOUDWATCH_CONFIG_HASH_TAG_BASE, config_type])
        filters = copy.deepcopy(
            self._get_current_cluster_session_nodes(self.cluster_name)
        )
        filters.append(
            {
                "Name": "tag:{}".format(TAG_RAY_NODE_KIND),
                "Values": [NODE_KIND_HEAD],
            }
        )
        try:
            instance = list(self.ec2_resource.instances.filter(Filters=filters))
            assert len(instance) == 1, "More than 1 head node found!"
            for tag in instance[0].tags:
                if tag["Key"] == hash_key_value:
                    return tag["Value"]
        except botocore.exceptions.ClientError as e:
            logger.warning(
                "{} Error caught when getting value of {} tag on head node".format(
                    e.response["Error"], hash_key_value
                )
            )

    def _get_cur_node_config_hash(self, config_type: str) -> str:
        hash_key_value = "-".join([CLOUDWATCH_CONFIG_HASH_TAG_BASE, config_type])
        try:
            response = self.ec2_client.describe_instances(InstanceIds=[self.node_id])
            reservations = response["Reservations"]
            message = "More than 1 response received from " "describing current node"
            assert len(reservations) == 1, message
            instances = reservations[0]["Instances"]
            assert len(reservations) == 1, message
            tags = instances[0]["Tags"]
            hash_value = self._get_default_empty_config_file_hash()
            for tag in tags:
                if tag["Key"] == hash_key_value:
                    logger.info(
                        "Successfully get cloudwatch {} hash tag value from "
                        "node {}".format(config_type, self.node_id)
                    )
                    hash_value = tag["Value"]
            return hash_value
        except botocore.exceptions.ClientError as e:
            logger.warning(
                "{} Error caught when getting hash tag {} tag".format(
                    e.response["Error"], hash_key_value
                )
            )

    def _ensure_cwa_installed_ssm(self, node_id: str) -> bool:
        """
        Check if Unified Cloudwatch Agent is installed via ssm run command.
        If not, notify user to use an AMI with
        the Unified CloudWatch Agent installed.
        """
        logger.info(
            "Checking Unified Cloudwatch Agent " "status on node {}".format(node_id)
        )
        parameters_status_cwa = {
            "action": ["status"],
            "mode": ["ec2"],
        }
        self._ec2_health_check_waiter(node_id)
        cmd_invocation_res = self._ssm_command_waiter(
            "AmazonCloudWatch-ManageAgent", parameters_status_cwa, node_id, False
        )
        cwa_installed = cmd_invocation_res.get(node_id, False)
        if not cwa_installed:
            logger.warning(
                "Unified CloudWatch Agent not installed on {}. "
                "Ray logs, metrics not picked up. "
                "Please use an AMI with Unified CloudWatch Agent installed.".format(
                    node_id
                )
            )
            return False
        else:
            return True

    def _get_current_cluster_session_nodes(self, cluster_name: str) -> List[dict]:
        filters = [
            {
                "Name": "instance-state-name",
                "Values": ["pending", "running"],
            },
            {
                "Name": "tag:{}".format(TAG_RAY_CLUSTER_NAME),
                "Values": [cluster_name],
            },
        ]
        return filters

    def _check_cwa_installed_ec2_tag(self) -> List[str]:
        """
        Filtering all nodes to get nodes
        without Unified CloudWatch Agent installed
        """
        try:
            response = self.ec2_client.describe_instances(InstanceIds=[self.node_id])
            reservations = response["Reservations"]
            message = "More than 1 response received from " "describing current node"
            assert len(reservations) == 1, message
            instances = reservations[0]["Instances"]
            assert len(instances) == 1, message
            tags = instances[0]["Tags"]
            cwa_installed = str(False)
            for tag in tags:
                if tag["Key"] == CLOUDWATCH_AGENT_INSTALLED_TAG:
                    logger.info(
                        "Unified CloudWatch Agent is installed on "
                        "node {}".format(self.node_id)
                    )
                    cwa_installed = tag["Value"]
            return cwa_installed
        except botocore.exceptions.ClientError as e:
            logger.warning(
                "{} Error caught when getting Unified CloudWatch Agent "
                "status based on {} tag".format(
                    e.response["Error"], CLOUDWATCH_AGENT_INSTALLED_TAG
                )
            )

    @staticmethod
    def resolve_instance_profile_name(
        config: Dict[str, Any], default_instance_profile_name: str
    ) -> str:
        """Get default cloudwatch instance profile name.

        Args:
            config: provider section of cluster config file.
            default_instance_profile_name: default ray instance profile name.

        Returns:
            default cloudwatch instance profile name if cloudwatch config file
                exists.
            default ray instance profile name if cloudwatch config file
                doesn't exist.
        """
        cwa_cfg_exists = CloudwatchHelper.cloudwatch_config_exists(config, "agent")
        return (
            CLOUDWATCH_RAY_INSTANCE_PROFILE
            if cwa_cfg_exists
            else default_instance_profile_name
        )

    @staticmethod
    def resolve_iam_role_name(
        config: Dict[str, Any], default_iam_role_name: str
    ) -> str:
        """Get default cloudwatch iam role name.

        Args:
            config: provider section of cluster config file.
            default_iam_role_name: default ray iam role name.

        Returns:
            default cloudwatch iam role name if cloudwatch config file exists.
            default ray iam role name if cloudwatch config file doesn't exist.
        """
        cwa_cfg_exists = CloudwatchHelper.cloudwatch_config_exists(config, "agent")
        return CLOUDWATCH_RAY_IAM_ROLE if cwa_cfg_exists else default_iam_role_name

    @staticmethod
    def resolve_policy_arns(
        config: Dict[str, Any], iam: Any, default_policy_arns: List[str]
    ) -> List[str]:
        """Attach necessary AWS policies for CloudWatch related operations.

        Args:
            config: provider section of cluster config file.
            iam: AWS iam resource.
            default_policy_arns: List of default ray AWS policies.

        Returns:
            list of policy arns including additional policies for CloudWatch
                related operations if cloudwatch agent config is specifed in
                cluster config file.
        """
        cwa_cfg_exists = CloudwatchHelper.cloudwatch_config_exists(config, "agent")
        if cwa_cfg_exists:
            cloudwatch_managed_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "ssm:SendCommand",
                            "ssm:ListCommandInvocations",
                            "iam:PassRole",
                        ],
                        "Resource": "*",
                    }
                ],
            }
            iam_client = iam.meta.client
            iam_client.create_policy(
                PolicyName="CloudwatchManagedPolicies",
                PolicyDocument=json.dumps(cloudwatch_managed_policy),
            )
            sts_client = client_cache("sts", config["region"])
            account_id = sts_client.get_caller_identity().get("Account")
            managed_policy_arn = (
                "arn:aws:iam::{}:policy/CloudwatchManagedPolicies".format(account_id)
            )
            policy_waiter = iam_client.get_waiter("policy_exists")
            policy_waiter.wait(
                PolicyArn=managed_policy_arn,
                WaiterConfig={"Delay": 2, "MaxAttempts": 200},
            )
            new_policy_arns = copy.copy(default_policy_arns)
            new_policy_arns.extend(
                [
                    "arn:aws:iam::aws:policy/CloudWatchAgentAdminPolicy",
                    "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
                    managed_policy_arn,
                ]
            )
            return new_policy_arns
        else:
            return default_policy_arns

    @staticmethod
    def cloudwatch_config_exists(config: Dict[str, Any], config_type: str) -> bool:
        """Check if CloudWatch configuration was specified by the user
        in their cluster config file.

        Specifically, this function checks if a CloudWatch config file is
        specified by the user in their cluster config file.

        Args:
            config: provider section of cluster config file.
            config_type: type of CloudWatch config file.

        Returns:
            True if config file is specified by user.
            False if config file is not specified.
        """
        cfg = config.get("cloudwatch", {}).get(config_type, {}).get("config")
        return bool(cfg)
