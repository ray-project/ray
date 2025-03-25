# Ray on vSphere Architecture Guide

To support ray on vSphere, the implementation has been added into [python/ray/autoscaler/_private/vsphere](../vsphere) directory. The following sections will explain the vSphere terminologies used in the code and also explain the whole code flow.


# vSphere Terminologies
## [OVF file](https://docs.vmware.com/en/VMware-vSphere/7.0/com.vmware.vsphere.vm_admin.doc/GUID-AE61948B-C2EE-436E-BAFB-3C7209088552.html)
OVF format is a packaging and distribution format for virtual machines. It is a standard which can be used to describe the VM metadata. We use the OVF files to create the virtual machines which will act as Ray head and worker node. 

## VI Admin

The term VI stands for [Virtual Infrastructure](https://www.vmware.com/in/topics/glossary/content/virtual-infrastructure.html).

A VI Admin is used to describe a persona that manages the lifecycle of VMware infrastructure. VI Admins engage in a range of activities. A subset of them are listed below:
1. Provisioning [ESXi](https://www.vmware.com/in/products/esxi-and-esx.html) (Hypervisor developed by VMware) hosts.
2. Provisioning a vSphere infrastructure.
3. Managing lifecycle of VMs.
4. Provisioning [vSAN](https://docs.vmware.com/en/VMware-vSAN/index.html) storage.  

# Code Flow

## Node Creation on `ray up`
The following sections explain the code flow in a sequential manner. The execution is triggered from the moment user executed `ray up` command

### Inject private Key ([config.py](./config.py))
During running `ray up`, the private key is injected into `config["auth"]["ssh_private_key"]`. The bootstrap machine (where the `ray up` command is executed) and the head node subsequently use this key to SSH onto the ray worker nodes.

### Update vSphere Configs ([config.py](./config.py))
Used to make sure that the user has created the YAML file with valid configs.

### Create Nodes ([node_provider.py](./cluster_operator_client.py))

#### Call `create_node`
Starts the creation of nodes with `create_node` function, which internally calls `_create_node`.

## Autoscaling

### Get and create nodes ([node_provider.py](./cluster_operator_client.py))
The autoscaler can find the currently running nodes with `non_terminated_nodes` function and can request for new nodes by calling `create_node` function.

### Fetch node IPs ([node_provider.py](./cluster_operator_client.py))
The autoscaler can use `external_ip` or `internal_ip` function to fetch a node's IP.

## Cluster tear down ([node_provider.py](./cluster_operator_client.py))
`terminate_nodes` function gets called on ray down command's execution. It deletes all the nodes.