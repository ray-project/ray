.. include:: we_are_hiring.rst

.. _gen2-cluster:

IBM Virtual Private Cloud
-------------------

Set up cluster configuration file manually
~~~~~~~~~~~~~~~~

The assumption that you already familiar with IBM Cloud, have your IBM IAM API key created (you can create new keys [here](https://cloud.ibm.com/iam/apikeys)), have valid IBM COS account, region and resource group.

Follow [IBM VPC setup](https://cloud.ibm.com/vpc-ext/overview) if you need to create IBM Virtual Private Cloud. Decide the region for your VPC. The best practice is to use the same region both for VPC and IBM COS, hoewever there is no requirement to keep them in the same region.

The following are the minimum setup requirements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Create new VPC if you don't have one already. More details [here](https://cloud.ibm.com/vpc-ext/network/vpcs)
2. Create new subnet with public gateway and IP range and total count. More details [here](https://cloud.ibm.com/vpc-ext/network/subnets)
3. Create new access contol list. More details [here](https://cloud.ibm.com/vpc-ext/network/acl)
4. Create security group for your resource group. More details [here](https://cloud.ibm.com/vpc-ext/network/securityGroups)
5. Create a SSH key in [IBM VPC SSH keys UI](https://cloud.ibm.com/vpc-ext/compute/sshKeys)
6. Choose an operating system image for VSI. Currently tested only with Ubuntu 20.04

Edit relevant sections in your `ray/python/ray/autoscaler/gen2/example-full.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/gen2/example-full.yaml>` and add the relevant keys:

.. code-block:: yaml
    auth:
    ssh_private_key: <SSH_PRIVATE_KEY_PATH>
    ssh_user: root
    available_node_types:
    worker_node:
        node_config:
        boot_volume_capacity: 100
        image_id: <IMAGE_ID>
        instance_profile_name: bx2-2x8
        key_id: <PUBLIC_KEY_ID>
        resource_group_id: <RESOURCE_GROUP_ID>
        security_group_id: <SECURITY_GROUP_ID>
        subnet_id: <SUBNET_ID>
        volume_tier_name: general-purpose
        vpc_id: <VPC_ID>
        resources:
        CPU: 2
    head_node:
        max_workers: 0
        min_workers: 0
        node_config:
        boot_volume_capacity: 100
        image_id: <IMAGE_ID>
        instance_profile_name: bx2-2x8
        key_id: <PUBLIC_KEY_ID>
        resource_group_id: <RESOURCE_GROUP_ID>
        security_group_id: <SECURITY_GROUP_ID>
        subnet_id: <SUBNET_ID>
        volume_tier_name: general-purpose
        vpc_id: <VPC_ID>
        resources:
        CPU: 2
    provider:
    cache_stopped_nodes: false
    endpoint: <REGION_ENDPOINT>
    iam_api_key: <API_KEY>
    region: <REGION_NAME>
    type: gen2
    use_hybrid_ips: true
    zone_name: <ZONE_NAME>

The fastest way to find all the required keys for `ibm_vpc` section as follows:

1. Login to IBM Cloud and open up your [dashboard](https://cloud.ibm.com/).
2. Navigate to your [IBM VPC create instance](https://cloud.ibm.com/vpc-ext/provision/vs).
3. On the left, fill all the parameters required for your new VM instance creation: name, resource group, location, ssh key, vpc. Choose either Ubuntu 20.04 VSI standard image or choose your **custom image** from the previous step
4. On the right, click `Get sample API call`.
5. Copy to clipboard the code from the `REST request: Creating a virtual server instance` dialog and paste to your favorite editor.
6. Close the `Create instance` window without creating it.
7. In the code, find `security_groups` section and paste its `id` value to the config file
8. Find `subnet` section and paste its `id` value to the config file
9. Find `keys` section and paste its `id` value to the config file
10. Find `resource_group` section and paste its `id` value to the config file
11. Find `vpc` section and paste its `id` value to the config file