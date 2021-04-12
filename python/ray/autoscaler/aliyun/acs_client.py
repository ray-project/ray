import logging
import json

from aliyunsdkcore import client
from aliyunsdkcore.acs_exception.exceptions import ClientException, ServerException
from aliyunsdkecs.request.v20140526.AddTagsRequest import AddTagsRequest
from aliyunsdkecs.request.v20140526.CreateInstanceRequest import CreateInstanceRequest
from aliyunsdkecs.request.v20140526.DeleteInstanceRequest import DeleteInstanceRequest
from aliyunsdkecs.request.v20140526.StartInstanceRequest import StartInstanceRequest
from aliyunsdkecs.request.v20140526.StopInstanceRequest import StopInstanceRequest
from aliyunsdkecs.request.v20140526.DescribeInstancesRequest import DescribeInstancesRequest


class AcsClient:
    def __init__(self, access_key, access_key_secret, region, max_retries):
        self.cli = client.AcsClient(
            ak=access_key,
            secret=access_key_secret,
            max_retry_time=max_retries,
            region_id=region,
        )

    def describe_instances(self, tags=None, instance_ids=None):
        request = DescribeInstancesRequest()
        if tags is not None:
            request.set_Tags(tags)
        if instance_ids is not None:
            request.set_InstanceIds(instance_ids)
        response = self._send_request(request)
        if response is not None:
            instance_list = response.get('Instances').get('Instance')
            return instance_list
        return None

    def create_instance(self, instance_type):
        request = CreateInstanceRequest()
        request.set_InstanceType(instance_type)
        request.set_IoOptimized('optimized')

        response = self._send_request(request)
        if response is not None:
            instance_id = response.get('InstanceId')
            logging.info("instance %s created task submit successfully.", instance_id)
            return instance_id
        logging.error("instance created failed.")
        return None

    def add_tags(self, instance_id, tags):
        request = AddTagsRequest()
        request.set_Tags(tags)
        response = self._send_request(request)
        if response is not None:
            logging.info("instance %s create tag successfully.", instance_id)
        else:
            logging.error("instance %s create tag failed.")

    def start_instance(self, instance_id):
        request = StartInstanceRequest()
        request.set_InstanceId(instance_id)
        response = self._send_request(request)

        if response is not None:
            logging.info("instance %s start successfully.", instance_id)
        else:
            logging.error("instance %s start failed.", instance_id)

    def stop_instance(self, instance_id, force_stop=False):
        request = StopInstanceRequest()
        request.set_InstanceId(instance_id)
        request.set_ForceStop(force_stop)
        logging.info("Stop %s command submit successfully.", instance_id)
        self._send_request(request)

    def delete_instance(self, instance_id):
        request = DeleteInstanceRequest()
        request.set_InstanceId(instance_id)
        logging.info("Delete %s command submit successfully", instance_id)
        self._send_request(instance_id)

    def _send_request(self, request):
        """send open api"""
        request.set_accept_format('json')
        try:
            response_str = self.cli.do_action_with_exception(request)
            response_detail = json.loads(response_str)
            return response_detail
        except ClientException as e1:
            logging.error(e1)
        except ServerException as e2:
            logging.error(e2)

