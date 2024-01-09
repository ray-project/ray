# Ray on vSphere Architecture Guide

To support ray on vSphere, the implementation has been added into [python/ray/autoscaler/_private/vsphere](../vsphere) directory. The following sections will explain the vSphere terminologies used in the code and also explain the whole code flow.


# vSphere Terminologies
## [OVF file](https://docs.vmware.com/en/VMware-vSphere/7.0/com.vmware.vsphere.vm_admin.doc/GUID-AE61948B-C2EE-436E-BAFB-3C7209088552.html)
OVF format is a packaging and distribution format for virtual machines. It is a standard which can be used to describe the VM metadata. We use the OVF files to create the [Frozen VM](#frozen-vm)

## Frozen VM
This is a VM that is kept in a frozen state i.e the clock of the VM is stopped. A VM in such a state can be used to create child VMs very rapidly with [instant clone](#instant-clone) operation.

The frozen VM itself is created from an OVF file. This OVF file executes a script on start of the VM that puts it into a frozen state. The script has the following sequence of execution at a high level:

 1. Execute `vmware-rpctool "instantclone.freeze"` command --> Puts the VM into the frozen state
 2. Reset the network

The script varies depending upon the Guest OS type. Sample scripts for various OSes can be found at the following github repo: [Instant Clone Customization scripts](https://github.com/lamw/instantclone-community-customization-scripts)
## [Instant Clone](https://docs.vmware.com/en/VMware-vSphere/7.0/com.vmware.vsphere.vm_admin.doc/GUID-853B1E2B-76CE-4240-A654-3806912820EB.html)
Instant clone feature of the vSphere can be used to quickly create new nodes by cloning from the frozen VM. The new nodes replicate the parent VM and continue execution post `vmware-rpctool "instantclone.freeze"` command i.e the cloned nodes reset their network to get new IP addresses.

## [Resource Pool](https://docs.vmware.com/en/VMware-vSphere/8.0/vsphere-resource-management/GUID-60077B40-66FF-4625-934A-641703ED7601.html)
Resource Pool is a logical abstraction that can be used to separate a group of VMs from others. It can also be configured to limit the resources that can be consumed by the VMs.

## [Datastore](https://docs.vmware.com/en/VMware-vSphere/7.0/com.vmware.vsphere.storage.doc/GUID-3CC7078E-9C30-402C-B2E1-2542BEE67E8F.html)

Datastores are logical containers that provide an uniform way to store the artifacts required by VMs. 

## VI Admin

The term VI stands for [Virtual Infrastructure](https://www.vmware.com/in/topics/glossary/content/virtual-infrastructure.html).

A VI Admin is used to describe a persona that manages the lifecycle of VMware infrastructure. VI Admins engage in a range of activities. A subset of them are listed below:
1. Provisioning [ESXi](https://www.vmware.com/in/products/esxi-and-esx.html) (Hypervisor developed by VMware) hosts.
2. Provisioning a vSphere infrastructure.
3. Managing lifecycle of VMs.
4. Provisioning [vSAN](https://docs.vmware.com/en/VMware-vSAN/index.html) storage.

## [vSphere Tags](https://docs.vmware.com/en/VMware-vSphere/8.0/vsphere-vcenter-esxi-management/GUID-16422FF7-235B-4A44-92E2-532F6AED0923.html#:~:text=You%20can%20create%2C%20edit%2C%20and,objects%20in%20the%20vSphere%20inventory)
A tag is a label that can be assigned to objects on the vSphere inventory. A tag needs to be assigned to a tag category.
A category allows to group tags together.

# Code Flow

## Node Creation on `ray up`
The following sections explain the code flow in a sequential manner. The execution is triggered from the moment user executed `ray up` command

### Inject private Key ([config.py](./config.py))
When defining the frozen VM using the [VM Packer for Ray](https://github.com/vmware-ai-labs/vm-packer-for-ray/blob/main/create-frozen-vm.sh#L48) project, the script should have generated a pair of keys under your home folder. During running `ray up`, the private key is injected into `config["auth"]["ssh_private_key"]`. The bootstrap machine (where the `ray up` command is executed) and the head node subsequently use this key to SSH onto the ray worker nodes.

### Update vSphere Configs ([config.py](./config.py))
Used to make sure that the user has created the YAML file with valid configs.

### Create Nodes ([node_provider.py](./node_provider.py))

#### Call `create_node`
Starts the creation of nodes with `create_node` function, which internally calls `_create_node`. The nodes are created in parallel. 

#### Fetch frozen VM
The frozen VM can be setup by the [VI admin](#vi-admin) using [VM Packer for Ray](https://github.com/vmware-ai-labs/vm-packer-for-ray). That tool can help to deploy a single frozen VM on the vSphere environment. Or optionally, fully clone the frozen VM to every ESXi hosts under a given resource pool. 

The vSphere Node Provider can take a frozen VM name or a resource pool name of a set of frozen VMs. If a resource pool name is given, the vSphere Node Provider will pick the most feasible frozen VM to do instant cloning.

Optionally, the vSphere Node Provider can also take a content library item name, then deploy the frozen VM before creating the Ray nodes. Details can be checked [here](https://docs.ray.io/en/latest/cluster/vms/references/ray-cluster-configuration.html?highlight=Cluster%20YAML%20Configuration%20Options#vsphere-config-frozen-vm).

#### Instant clone the nodes
All the nodes are instantly cloned from the frozen VM. 

#### Tag nodes with [vSphere Tags](#vsphere-tags)
The nodes are tagged while their creation is in progress in an async way with `tag_new_vm_instantly` function.
Post creation of the nodes, the tags on the nodes are updated. 

## Autoscaling

### Get and create nodes ([node_provider.py](./node_provider.py))
The autoscaler can find the currently running nodes with `non_terminated_nodes` function and can request for new nodes by calling `create_node` function.

### Fetch node IPs ([node_provider.py](./node_provider.py))
The autoscaler can use `external_ip` or `internal_ip` function to fetch a node's IP.

## Cluster tear down ([node_provider.py](./node_provider.py))
`terminate_nodes` function gets called on ray down command's execution. It deletes all the nodes except the frozen VM.