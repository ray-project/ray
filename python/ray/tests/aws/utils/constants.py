import copy
import ray
from datetime import datetime

# Override global constants used in AWS autoscaler config artifact names.
# This helps ensure that any unmocked test doesn't alter non-test artifacts.
ray.autoscaler._private.aws.config.RAY = \
    "ray-autoscaler-aws-test"
ray.autoscaler._private.aws.config.DEFAULT_RAY_INSTANCE_PROFILE = \
    ray.autoscaler._private.aws.config.RAY + "-v1"
ray.autoscaler._private.aws.config.DEFAULT_RAY_IAM_ROLE = \
    ray.autoscaler._private.aws.config.RAY + "-v1"
ray.autoscaler._private.aws.config.SECURITY_GROUP_TEMPLATE = \
    ray.autoscaler._private.aws.config.RAY + "-{}"

# Default IAM instance profile to expose to tests.
DEFAULT_INSTANCE_PROFILE = {
    "Arn": "arn:aws:iam::336924118301:instance-profile/ExampleInstanceProfile",
    "CreateDate": datetime(2013, 6, 12, 23, 52, 2, 2),
    "InstanceProfileId": "AIPA0000000000EXAMPLE",
    "InstanceProfileName": "ExampleInstanceProfile",
    "Path": "/",
    "Roles": [
        {
            "Arn": "arn:aws:iam::123456789012:role/Test-Role",
            "AssumeRolePolicyDocument": "ExampleAssumeRolePolicyDocument",
            "CreateDate": datetime(2013, 1, 9, 6, 33, 26, 2),
            "Path": "/",
            "RoleId": "AROA0000000000EXAMPLE",
            "RoleName": "Test-Role",
        },
    ]
}

# Default EC2 key pair to expose to tests.
DEFAULT_KEY_PAIR = {
    "KeyFingerprint": "00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00",
    "KeyName": ray.autoscaler._private.aws.config.RAY + "_us-west-2",
}

# Primary EC2 subnet to expose to tests.
DEFAULT_SUBNET = {
    "AvailabilityZone": "us-west-2a",
    "AvailableIpAddressCount": 251,
    "CidrBlock": "10.0.1.0/24",
    "DefaultForAz": False,
    "MapPublicIpOnLaunch": True,
    "State": "available",
    "SubnetId": "subnet-0000000",
    "VpcId": "vpc-0000000",
}


def subnet_in_vpc(vpc_num):
    """Returns a copy of DEFAULT_SUBNET whose VpcId ends with the digits
    of vpc_num."""
    subnet = copy.copy(DEFAULT_SUBNET)
    subnet["VpcId"] = f"vpc-{vpc_num:07d}"
    return subnet


A_THOUSAND_SUBNETS_IN_DIFFERENT_VPCS = [
    subnet_in_vpc(vpc_num) for vpc_num in range(1, 1000)
] + [DEFAULT_SUBNET]

# Secondary EC2 subnet to expose to tests as required.
AUX_SUBNET = {
    "AvailabilityZone": "us-west-2a",
    "AvailableIpAddressCount": 251,
    "CidrBlock": "192.168.1.0/24",
    "DefaultForAz": False,
    "MapPublicIpOnLaunch": True,
    "State": "available",
    "SubnetId": "subnet-fffffff",
    "VpcId": "vpc-fffffff",
}

# Default cluster name to expose to tests.
DEFAULT_CLUSTER_NAME = "test-cluster-name"

# Default security group settings immediately after creation
# (prior to inbound rule configuration).
DEFAULT_SG = {
    "Description": "Auto-created security group for Ray workers",
    "GroupName": ray.autoscaler._private.aws.config.RAY + "-" +
    DEFAULT_CLUSTER_NAME,
    "OwnerId": "test-owner",
    "GroupId": "sg-1234abcd",
    "VpcId": DEFAULT_SUBNET["VpcId"],
    "IpPermissions": [],
    "IpPermissionsEgress": [{
        "FromPort": -1,
        "ToPort": -1,
        "IpProtocol": "-1",
        "IpRanges": [{
            "CidrIp": "0.0.0.0/0"
        }]
    }],
    "Tags": []
}

# Secondary security group settings after creation
# (prior to inbound rule configuration).
AUX_SG = copy.deepcopy(DEFAULT_SG)
AUX_SG["GroupName"] += "-aux"
AUX_SG["GroupId"] = "sg-dcba4321"

# Default security group settings immediately after creation on aux subnet
# (prior to inbound rule configuration).
DEFAULT_SG_AUX_SUBNET = copy.deepcopy(DEFAULT_SG)
DEFAULT_SG_AUX_SUBNET["VpcId"] = AUX_SUBNET["VpcId"]
DEFAULT_SG_AUX_SUBNET["GroupId"] = AUX_SG["GroupId"]

DEFAULT_IN_BOUND_RULES = [{
    "FromPort": -1,
    "ToPort": -1,
    "IpProtocol": "-1",
    "UserIdGroupPairs": [{
        "GroupId": DEFAULT_SG["GroupId"]
    }]
}, {
    "FromPort": 22,
    "ToPort": 22,
    "IpProtocol": "tcp",
    "IpRanges": [{
        "CidrIp": "0.0.0.0/0"
    }]
}]
# Default security group settings once default inbound rules are applied
# (if used by both head and worker nodes)
DEFAULT_SG_WITH_RULES = copy.deepcopy(DEFAULT_SG)
DEFAULT_SG_WITH_RULES["IpPermissions"] = DEFAULT_IN_BOUND_RULES

# Default security group once default inbound rules are applied
# (if using separate security groups for head and worker nodes).
DEFAULT_SG_DUAL_GROUP_RULES = copy.deepcopy(DEFAULT_SG_WITH_RULES)
DEFAULT_SG_DUAL_GROUP_RULES["IpPermissions"][0]["UserIdGroupPairs"].append({
    "GroupId": AUX_SG["GroupId"]
})

# Default security group on aux subnet once default inbound rules are applied.
DEFAULT_SG_WITH_RULES_AUX_SUBNET = copy.deepcopy(DEFAULT_SG_DUAL_GROUP_RULES)
DEFAULT_SG_WITH_RULES_AUX_SUBNET["VpcId"] = AUX_SUBNET["VpcId"]
DEFAULT_SG_WITH_RULES_AUX_SUBNET["GroupId"] = AUX_SG["GroupId"]

# Default security group with custom name
DEFAULT_SG_WITH_NAME = copy.deepcopy(DEFAULT_SG)
DEFAULT_SG_WITH_NAME["GroupName"] = "test_security_group_name"

CUSTOM_IN_BOUND_RULES = [{
    "FromPort": 443,
    "ToPort": 443,
    "IpProtocol": "TCP",
    "IpRanges": [{
        "CidrIp": "0.0.0.0/0"
    }]
}, {
    "FromPort": 8265,
    "ToPort": 8265,
    "IpProtocol": "TCP",
    "IpRanges": [{
        "CidrIp": "0.0.0.0/0"
    }]
}]

# Default security group with custom name once...
# default and custom in bound rules are applied
DEFAULT_SG_WITH_NAME_AND_RULES = copy.deepcopy(DEFAULT_SG_WITH_NAME)
DEFAULT_SG_WITH_NAME_AND_RULES[
    "IpPermissions"] = DEFAULT_IN_BOUND_RULES + CUSTOM_IN_BOUND_RULES
