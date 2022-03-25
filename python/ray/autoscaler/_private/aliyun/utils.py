import logging
import json

from aliyunsdkcore import client
from aliyunsdkcore.acs_exception.exceptions import ClientException, ServerException
from aliyunsdkecs.request.v20140526.CreateInstanceRequest import CreateInstanceRequest
from aliyunsdkecs.request.v20140526.DeleteInstanceRequest import DeleteInstanceRequest
from aliyunsdkecs.request.v20140526.DeleteInstancesRequest import DeleteInstancesRequest
from aliyunsdkecs.request.v20140526.StartInstanceRequest import StartInstanceRequest
from aliyunsdkecs.request.v20140526.StopInstanceRequest import StopInstanceRequest
from aliyunsdkecs.request.v20140526.StopInstancesRequest import StopInstancesRequest
from aliyunsdkecs.request.v20140526.DescribeInstancesRequest import (
    DescribeInstancesRequest,
)
from aliyunsdkecs.request.v20140526.TagResourcesRequest import TagResourcesRequest
from aliyunsdkecs.request.v20140526.AllocatePublicIpAddressRequest import (
    AllocatePublicIpAddressRequest,
)
from aliyunsdkecs.request.v20140526.ImportKeyPairRequest import ImportKeyPairRequest
from aliyunsdkecs.request.v20140526.DescribeKeyPairsRequest import (
    DescribeKeyPairsRequest,
)
from aliyunsdkecs.request.v20140526.CreateKeyPairRequest import CreateKeyPairRequest
from aliyunsdkecs.request.v20140526.RunInstancesRequest import RunInstancesRequest
from aliyunsdkecs.request.v20140526.CreateSecurityGroupRequest import (
    CreateSecurityGroupRequest,
)
from aliyunsdkecs.request.v20140526.DeleteKeyPairsRequest import DeleteKeyPairsRequest
from aliyunsdkecs.request.v20140526.AuthorizeSecurityGroupRequest import (
    AuthorizeSecurityGroupRequest,
)
from aliyunsdkecs.request.v20140526.DescribeSecurityGroupsRequest import (
    DescribeSecurityGroupsRequest,
)
from aliyunsdkecs.request.v20140526.CreateVSwitchRequest import CreateVSwitchRequest
from aliyunsdkecs.request.v20140526.CreateVpcRequest import CreateVpcRequest
from aliyunsdkecs.request.v20140526.DescribeVpcsRequest import DescribeVpcsRequest
from aliyunsdkecs.request.v20140526.DescribeVSwitchesRequest import (
    DescribeVSwitchesRequest,
)


class AcsClient:
    """
    A wrapper around Aliyun SDK. We use this wrapper in aliyun node provider.

    Parameters:
        access_key: The AccessKey ID of your aliyun account.
        access_key_secret: The AccessKey secret of your aliyun account.
        region_id: A region is a geographic area where a data center resides.
                   Region_id is the ID of region (e.g., cn-hangzhou,
                   us-west-1, etc.)
        max_retries: The maximum number of retries each connection.
    """

    def __init__(self, access_key, access_key_secret, region_id, max_retries):
        self.cli = client.AcsClient(
            ak=access_key,
            secret=access_key_secret,
            max_retry_time=max_retries,
            region_id=region_id,
        )

    def describe_instances(self, tags=None, instance_ids=None):
        """Query the details of one or more Elastic Compute Service (ECS) instances.

        :param tags: The tags of the instance.
        :param instance_ids: The IDs of ECS instances
        :return: ECS instance list
        """
        request = DescribeInstancesRequest()
        if tags is not None:
            request.set_Tags(tags)
        if instance_ids is not None:
            request.set_InstanceIds(instance_ids)
        response = self._send_request(request)
        if response is not None:
            instance_list = response.get("Instances").get("Instance")
            return instance_list
        return None

    def create_instance(
        self,
        instance_type,
        image_id,
        tags,
        key_pair_name,
        optimized="optimized",
        instance_charge_type="PostPaid",
        spot_strategy="SpotWithPriceLimit",
        internet_charge_type="PayByTraffic",
        internet_max_bandwidth_out=5,
    ):
        """Create a subscription or pay-as-you-go ECS instance.

        :param instance_type: The instance type of the ECS.
        :param image_id: The ID of the image used to create the instance.
        :param tags: The tags of the instance.
        :param key_pair_name: The name of the key pair to be bound to
                              the instance.
        :param optimized: Specifies whether the instance is I/O optimized
        :param instance_charge_type: The billing method of the instance.
                                     Default value: PostPaid.
        :param spot_strategy: The preemption policy for the pay-as-you-go
                              instance.
        :param internet_charge_type: The billing method for network usage.
                                     Default value: PayByTraffic.
        :param internet_max_bandwidth_out: The maximum inbound public
                                           bandwidth. Unit: Mbit/s.
        :return: The created instance ID.
        """
        request = CreateInstanceRequest()
        request.set_InstanceType(instance_type)
        request.set_ImageId(image_id)
        request.set_IoOptimized(optimized)
        request.set_InstanceChargeType(instance_charge_type)
        request.set_SpotStrategy(spot_strategy)
        request.set_InternetChargeType(internet_charge_type)
        request.set_InternetMaxBandwidthOut(internet_max_bandwidth_out)
        request.set_KeyPairName(key_pair_name)
        request.set_Tags(tags)

        response = self._send_request(request)
        if response is not None:
            instance_id = response.get("InstanceId")
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
        optimized="optimized",
        instance_charge_type="PostPaid",
        spot_strategy="SpotWithPriceLimit",
        internet_charge_type="PayByTraffic",
        internet_max_bandwidth_out=1,
    ):
        """Create one or more pay-as-you-go or subscription
            Elastic Compute Service (ECS) instances

        :param instance_type: The instance type of the ECS.
        :param image_id: The ID of the image used to create the instance.
        :param tags: The tags of the instance.
        :param security_group_id: The ID of the security group to which to
                                  assign the instance. Instances in the same
                                  security group can communicate with
                                  each other.
        :param vswitch_id: The ID of the vSwitch to which to connect
                           the instance.
        :param key_pair_name: The name of the key pair to be bound to
                              the instance.
        :param amount: The number of instances that you want to create.
        :param optimized: Specifies whether the instance is I/O optimized
        :param instance_charge_type: The billing method of the instance.
                                     Default value: PostPaid.
        :param spot_strategy: The preemption policy for the pay-as-you-go
                              instance.
        :param internet_charge_type: The billing method for network usage.
                                     Default value: PayByTraffic.
        :param internet_max_bandwidth_out: The maximum inbound public
                                           bandwidth. Unit: Mbit/s.
        :return: The created instance IDs.
        """
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
            instance_ids = response.get("InstanceIdSets").get("InstanceIdSet")
            return instance_ids
        logging.error("instance created failed.")
        return None

    def create_security_group(self, vpc_id):
        """Create a security group

        :param vpc_id: The ID of the VPC in which to create
                       the security group.
        :return: The created security group ID.
        """
        request = CreateSecurityGroupRequest()
        request.set_VpcId(vpc_id)
        response = self._send_request(request)
        if response is not None:
            security_group_id = response.get("SecurityGroupId")
            return security_group_id
        return None

    def describe_security_groups(self, vpc_id=None, tags=None):
        """Query basic information of security groups.

        :param vpc_id: The ID of the VPC to which the security group belongs.
        :param tags: The tags of the security group.
        :return: Security group list.
        """
        request = DescribeSecurityGroupsRequest()
        if vpc_id is not None:
            request.set_VpcId(vpc_id)
        if tags is not None:
            request.set_Tags(tags)
        response = self._send_request(request)
        if response is not None:
            security_groups = response.get("SecurityGroups").get("SecurityGroup")
            return security_groups
        logging.error("describe security group failed.")
        return None

    def authorize_security_group(
        self, ip_protocol, port_range, security_group_id, source_cidr_ip
    ):
        """Create an inbound security group rule.

        :param ip_protocol: The transport layer protocol.
        :param port_range: The range of destination ports relevant to
                           the transport layer protocol.
        :param security_group_id: The ID of the destination security group.
        :param source_cidr_ip: The range of source IPv4 addresses.
                               CIDR blocks and IPv4 addresses are supported.
        """
        request = AuthorizeSecurityGroupRequest()
        request.set_IpProtocol(ip_protocol)
        request.set_PortRange(port_range)
        request.set_SecurityGroupId(security_group_id)
        request.set_SourceCidrIp(source_cidr_ip)
        self._send_request(request)

    def create_v_switch(self, vpc_id, zone_id, cidr_block):
        """Create vSwitches to divide the VPC into one or more subnets

        :param vpc_id: The ID of the VPC to which the VSwitch belongs.
        :param zone_id: The ID of the zone to which
                        the target VSwitch belongs.
        :param cidr_block: The CIDR block of the VSwitch.
        :return:
        """
        request = CreateVSwitchRequest()
        request.set_ZoneId(zone_id)
        request.set_VpcId(vpc_id)
        request.set_CidrBlock(cidr_block)
        response = self._send_request(request)
        if response is not None:
            return response.get("VSwitchId")
        else:
            logging.error("create_v_switch vpc_id %s failed.", vpc_id)
        return None

    def create_vpc(self):
        """Creates a virtual private cloud (VPC).

        :return: The created VPC ID.
        """
        request = CreateVpcRequest()
        response = self._send_request(request)
        if response is not None:
            return response.get("VpcId")
        return None

    def describe_vpcs(self):
        """Queries one or more VPCs in a region.

        :return: VPC list.
        """
        request = DescribeVpcsRequest()
        response = self._send_request(request)
        if response is not None:
            return response.get("Vpcs").get("Vpc")
        return None

    def tag_resource(self, resource_ids, tags, resource_type="instance"):
        """Create and bind tags to specified ECS resources.

        :param resource_ids: The IDs of N resources.
        :param tags: The tags of the resource.
        :param resource_type: The type of the resource.
        """
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
        """Start an ECS instance.

        :param instance_id: The Ecs instance ID.
        """
        request = StartInstanceRequest()
        request.set_InstanceId(instance_id)
        response = self._send_request(request)

        if response is not None:
            logging.info("instance %s start successfully.", instance_id)
        else:
            logging.error("instance %s start failed.", instance_id)

    def stop_instance(self, instance_id, force_stop=False):
        """Stop an ECS instance that is in the Running state.

        :param instance_id: The Ecs instance ID.
        :param force_stop: Specifies whether to forcibly stop the instance.
        :return:
        """
        request = StopInstanceRequest()
        request.set_InstanceId(instance_id)
        request.set_ForceStop(force_stop)
        logging.info("Stop %s command submit successfully.", instance_id)
        self._send_request(request)

    def stop_instances(self, instance_ids, stopped_mode="StopCharging"):
        """Stop one or more ECS instances that are in the Running state.

        :param instance_ids: The IDs of instances.
        :param stopped_mode: Specifies whether billing for the instance
                             continues after the instance is stopped.
        """
        request = StopInstancesRequest()
        request.set_InstanceIds(instance_ids)
        request.set_StoppedMode(stopped_mode)
        response = self._send_request(request)
        if response is None:
            logging.error("stop_instances failed")

    def delete_instance(self, instance_id):
        """Release a pay-as-you-go instance or
            an expired subscription instance.

        :param instance_id: The ID of the instance that you want to release.
        """
        request = DeleteInstanceRequest()
        request.set_InstanceId(instance_id)
        request.set_Force(True)
        logging.info("Delete %s command submit successfully", instance_id)
        self._send_request(request)

    def delete_instances(self, instance_ids):
        """Release one or more pay-as-you-go instances or
            expired subscription instances.

        :param instance_ids: The IDs of instances that you want to release.
        """
        request = DeleteInstancesRequest()
        request.set_Force(True)
        request.set_InstanceIds(instance_ids)
        self._send_request(request)

    def allocate_public_address(self, instance_id):
        """Assign a public IP address to an ECS instance.

        :param instance_id: The ID of the instance to which you want to
                            assign a public IP address.
        :return: The assigned ip.
        """
        request = AllocatePublicIpAddressRequest()
        request.set_InstanceId(instance_id)
        response = self._send_request(request)
        if response is not None:
            return response.get("IpAddress")

    def create_key_pair(self, key_pair_name):
        """Create an SSH key pair.

        :param key_pair_name: The name of the key pair.
        :return: The created keypair data.
        """
        request = CreateKeyPairRequest()
        request.set_KeyPairName(key_pair_name)
        response = self._send_request(request)
        if response is not None:
            logging.info("Create Key Pair %s Successfully", response.get("KeyPairId"))
            return response
        else:
            logging.error("Create Key Pair Failed")
            return None

    def import_key_pair(self, key_pair_name, public_key_body):
        """Import the public key of an RSA-encrypted key pair
            that is generated by a third-party tool.

        :param key_pair_name: The name of the key pair.
        :param public_key_body: The public key of the key pair.
        """
        request = ImportKeyPairRequest()
        request.set_KeyPairName(key_pair_name)
        request.set_PublicKeyBody(public_key_body)
        self._send_request(request)

    def delete_key_pairs(self, key_pair_names):
        """Delete one or more SSH key pairs.

        :param key_pair_names: The name of the key pair.
        :return:
        """
        request = DeleteKeyPairsRequest()
        request.set_KeyPairNames(key_pair_names)
        self._send_request(request)

    def describe_key_pairs(self, key_pair_name=None):
        """Query one or more key pairs.

        :param key_pair_name: The name of the key pair.
        :return:
        """
        request = DescribeKeyPairsRequest()
        if key_pair_name is not None:
            request.set_KeyPairName(key_pair_name)
        response = self._send_request(request)
        if response is not None:
            return response.get("KeyPairs").get("KeyPair")
        else:
            return None

    def describe_v_switches(self, vpc_id=None):
        """Queries one or more VSwitches.

        :param vpc_id: The ID of the VPC to which the VSwitch belongs.
        :return: VSwitch list.
        """
        request = DescribeVSwitchesRequest()
        if vpc_id is not None:
            request.set_VpcId(vpc_id)
        response = self._send_request(request)
        if response is not None:
            return response.get("VSwitches").get("VSwitch")
        else:
            logging.error("Describe VSwitches Failed.")
            return None

    def _send_request(self, request):
        """send open api request"""
        request.set_accept_format("json")
        try:
            response_str = self.cli.do_action_with_exception(request)
            response_detail = json.loads(response_str)
            return response_detail
        except (ClientException, ServerException) as e:
            logging.error(request.get_action_name())
            logging.error(e)
            return None
