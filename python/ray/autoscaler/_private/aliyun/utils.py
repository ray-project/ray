import logging
import json

from aliyunsdkcore import client
from aliyunsdkcore.acs_exception.exceptions import ClientException, ServerException
from aliyunsdkecs.request.v20140526.AddTagsRequest import AddTagsRequest
from aliyunsdkecs.request.v20140526.CreateInstanceRequest import CreateInstanceRequest
from aliyunsdkecs.request.v20140526.DeleteInstanceRequest import DeleteInstanceRequest
from aliyunsdkecs.request.v20140526.DeleteInstancesRequest import DeleteInstancesRequest
from aliyunsdkecs.request.v20140526.StartInstanceRequest import StartInstanceRequest
from aliyunsdkecs.request.v20140526.StopInstanceRequest import StopInstanceRequest
from aliyunsdkecs.request.v20140526.StopInstancesRequest import StopInstancesRequest
from aliyunsdkecs.request.v20140526.DescribeInstancesRequest import DescribeInstancesRequest
from aliyunsdkecs.request.v20140526.TagResourcesRequest import TagResourcesRequest
from aliyunsdkecs.request.v20140526.AllocatePublicIpAddressRequest import AllocatePublicIpAddressRequest
from aliyunsdkecs.request.v20140526.AttachKeyPairRequest import AttachKeyPairRequest
from aliyunsdkecs.request.v20140526.ImportKeyPairRequest import ImportKeyPairRequest
from aliyunsdkecs.request.v20140526.DescribeKeyPairsRequest import DescribeKeyPairsRequest
from aliyunsdkecs.request.v20140526.CreateKeyPairRequest import CreateKeyPairRequest
from aliyunsdkecs.request.v20140526.RunInstancesRequest import RunInstancesRequest
from aliyunsdkecs.request.v20140526.CreateSecurityGroupRequest import CreateSecurityGroupRequest
from aliyunsdkecs.request.v20140526.DeleteKeyPairsRequest import DeleteKeyPairsRequest
from aliyunsdkecs.request.v20140526.AuthorizeSecurityGroupRequest import AuthorizeSecurityGroupRequest
from aliyunsdkecs.request.v20140526.DescribeSecurityGroupsRequest import DescribeSecurityGroupsRequest
from aliyunsdkecs.request.v20140526.CreateVSwitchRequest import CreateVSwitchRequest
from aliyunsdkecs.request.v20140526.CreateVpcRequest import CreateVpcRequest
from aliyunsdkecs.request.v20140526.DescribeZonesRequest import DescribeZonesRequest
from aliyunsdkecs.request.v20140526.DescribeVpcsRequest import DescribeVpcsRequest
from aliyunsdkecs.request.v20140526.DescribeVSwitchesRequest import DescribeVSwitchesRequest
from aliyunsdkecs.request.v20140526.RebootInstanceRequest import RebootInstanceRequest


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

    def create_instance(
        self,
        instance_type,
        image_id,
        tags,
        optimized='optimized',
        instance_charge_type='PostPaid',
        spot_strategy='SpotWithPriceLimit',
        internet_charge_type='PayByTraffic',
        internet_max_bandwidth_out=5,
        key_pair_name='admin_id_rsa'
    ):
        request = CreateInstanceRequest()
        request.set_InstanceType(instance_type)
        request.set_ImageId(image_id)
        request.set_IoOptimized(optimized)
        request.set_InstanceChargeType(instance_charge_type)
        request.set_SpotStrategy(spot_strategy)
        request.set_InternetChargeType(internet_charge_type)
        request.set_InternetMaxBandwidthOut(internet_max_bandwidth_out)
        # request.set_KeyPairName(key_pair_name)
        request.set_Tags(tags)

        response = self._send_request(request)
        if response is not None:
            instance_id = response.get('InstanceId')
            logging.info("instance %s created task submit successfully.", instance_id)
            return instance_id
        logging.error("instance created failed.")
        return None

    def run_instances(
        self,
        instance_type,
        image_id,
        tags,
        security_group_id,
        vswitch_id,
        key_pair_name,
        amount=1,
        optimized='optimized',
        instance_charge_type='PostPaid',
        spot_strategy='SpotWithPriceLimit',
        internet_charge_type='PayByTraffic',
        internet_max_bandwidth_out=1,
    ):
        request = RunInstancesRequest()
        request.set_InstanceType(instance_type)
        request.set_ImageId(image_id)
        request.set_IoOptimized(optimized)
        request.set_InstanceChargeType(instance_charge_type)
        request.set_SpotStrategy(spot_strategy)
        request.set_InternetChargeType(internet_charge_type)
        request.set_InternetMaxBandwidthOut(internet_max_bandwidth_out)
        request.set_Tags(tags)
        request.set_Amount(amount)
        request.set_SecurityGroupId(security_group_id)
        request.set_VSwitchId(vswitch_id)
        request.set_KeyPairName(key_pair_name)

        response = self._send_request(request)
        if response is not None:
            instance_ids = response.get('InstanceIdSets').get('InstanceIdSet')
            return instance_ids
        logging.error("instance created failed.")
        return None

    def create_security_group(self, vpc_id):
        request = CreateSecurityGroupRequest()
        request.set_VpcId(vpc_id)
        response = self._send_request(request)
        if response is not None:
            security_group_id = response.get('SecurityGroupId')
            return security_group_id
        return None

    def describe_security_groups(self, vpc_id=None, tags=None):
        request = DescribeSecurityGroupsRequest()
        if vpc_id is not None:
            request.set_VpcId(vpc_id)
        if tags is not None:
            request.set_Tags(tags)
        response = self._send_request(request)
        if response is not None:
            security_groups = response.get('SecurityGroups').get('SecurityGroup')
            return security_groups
        logging.error("describe security group failed.")
        return None

    def authorize_security_group(self, ip_protocol, port_range, security_group_id, source_cidr_ip):
        request = AuthorizeSecurityGroupRequest()
        request.set_IpProtocol(ip_protocol)
        request.set_PortRange(port_range)
        request.set_SecurityGroupId(security_group_id)
        request.set_SourceCidrIp(source_cidr_ip)
        self._send_request(request)

    def create_v_switch(self, vpc_id, zone_id, cidr_block):
        request = CreateVSwitchRequest()
        request.set_ZoneId(zone_id)
        request.set_VpcId(vpc_id)
        request.set_CidrBlock(cidr_block)
        response = self._send_request(request)
        if response is not None:
            return response.get('VSwitchId')
        else:
            logging.error("create_v_switch vpc_id %s failed.", vpc_id)
        return None

    def create_vpc(self):
        request = CreateVpcRequest()
        response = self._send_request(request)
        if response is not None:
            return response.get('VpcId')
        return None

    def describe_vpcs(self):
        request = DescribeVpcsRequest()
        response = self._send_request(request)
        if response is not None:
            return response.get('Vpcs').get('Vpc')
        return None

    def tag_resource(self, resource_ids, tags, resource_type='instance'):
        request = TagResourcesRequest()
        request.set_Tags(tags)
        request.set_ResourceType(resource_type)
        request.set_ResourceIds(resource_ids)
        response = self._send_request(request)
        if response is not None:
            logging.info("instance %s create tag successfully.", resource_ids)
        else:
            logging.error("instance %s create tag failed.", resource_ids)

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

    def stop_instances(self, instance_ids, stopped_mode='StopCharging'):
        request = StopInstancesRequest()
        request.set_InstanceIds(instance_ids)
        request.set_StoppedMode(stopped_mode)
        response = self._send_request(request)
        if response is not None:
            return response.get('InstanceResponses').get('InstanceResponse')
        logging.error("stop_instances failed")
        return None

    def reboot_instance(self, instance_id):
        request = RebootInstanceRequest()
        request.set_InstanceId(instance_id)
        self._send_request(request)
        return None

    def delete_instance(self, instance_id):
        request = DeleteInstanceRequest()
        request.set_InstanceId(instance_id)
        logging.info("Delete %s command submit successfully", instance_id)
        self._send_request(request)

    def delete_instances(self, instance_ids):
        request = DeleteInstancesRequest()
        request.set_InstanceIds(instance_ids)
        self._send_request(request)

    def allocate_public_address(self, instance_id):
        request = AllocatePublicIpAddressRequest()
        request.set_InstanceId(instance_id)
        response = self._send_request(request)
        if response is not None:
            return response.get('IpAddress')

    def create_key_pair(self, key_pair_name):
        request = CreateKeyPairRequest()
        request.set_KeyPairName(key_pair_name)
        response = self._send_request(request)
        if response is not None:
            logging.info("Create Key Pair %s Successfully", response.get('KeyPairId'))
            return response
        else:
            logging.error("Create Key Pair Failed")
            return None

    def import_key_pair(self, key_pair_name, public_key_body):
        request = ImportKeyPairRequest()
        request.set_KeyPairName(key_pair_name)
        request.set_PublicKeyBody(public_key_body)
        self._send_request(request)

    def delete_key_pairs(self, key_pair_names):
        request = DeleteKeyPairsRequest()
        request.set_KeyPairNames(key_pair_names)
        self._send_request(request)

    def describe_key_pairs(self, key_pair_name=None):
        request = DescribeKeyPairsRequest()
        if key_pair_name is not None:
            request.set_KeyPairName(key_pair_name)
        response = self._send_request(request)
        if response is not None:
            return response.get('KeyPairs').get('KeyPair')
        else:
            return None

    def describe_zones(self):
        request = DescribeZonesRequest()
        response = self._send_request(request)
        if response is not None:
            # print('describe zones: %s' % response)
            return response.get('Zones').get('Zone')
        return None

    def attach_key_pair(self, instance_ids, key_pair_name):
        request = AttachKeyPairRequest()
        request.set_InstanceIds(instance_ids)
        request.set_KeyPairName(key_pair_name)
        response = self._send_request(request)
        if response is not None:
            return response.get('Results').get('Result')
        else:
            logging.error("instance %s attach_key_pair failed.", instance_ids)
            return None

    def describe_v_switches(self, vpc_id=None):
        request = DescribeVSwitchesRequest()
        if vpc_id is not None:
            request.set_VpcId(vpc_id)
        response = self._send_request(request)
        if response is not None:
            return response.get('VSwitches').get('VSwitch')
        else:
            logging.error("Describe VSwitches Failed.")
            return None

    def _send_request(self, request):
        """send open api"""
        request.set_accept_format('json')
        try:
            response_str = self.cli.do_action_with_exception(request)
            response_detail = json.loads(response_str)
            return response_detail
        except ClientException as e1:
            logging.error(request.get_action_name())
            logging.error(e1)
            return None
        except ServerException as e2:
            logging.error(request.get_action_name())
            logging.error(e2)
            return None
