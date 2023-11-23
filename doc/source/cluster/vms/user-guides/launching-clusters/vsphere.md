# Launching Ray Clusters on vSphere

This guide details the steps needed to launch a Ray cluster in a vSphere environment.

To start a vSphere Ray cluster, you will use the Ray cluster launcher with the VMware vSphere Automation SDK for Python.

## Prepare the vSphere environment

If you don't already have a vSphere deployment, you can learn more about it by reading the [vSphere documentation](https://docs.vmware.com/en/VMware-vSphere/index.html). The vSphere Ray cluster launcher requires vSphere version 8.0 or later, along with the following prerequisites for creating Ray clusters.

* [A vSphere cluster](https://docs.vmware.com/en/VMware-vSphere/8.0/vsphere-vcenter-esxi-management/GUID-F7818000-26E3-4E2A-93D2-FCDCE7114508.html) and [resource pools](https://docs.vmware.com/en/VMware-vSphere/8.0/vsphere-resource-management/GUID-60077B40-66FF-4625-934A-641703ED7601.html) to host VMs composing Ray Clusters.
* A network port group (either for a [standard switch](https://docs.vmware.com/en/VMware-vSphere/8.0/vsphere-networking/GUID-E198C88A-F82C-4FF3-96C9-E3DF0056AD0C.html) or [distributed switch](https://docs.vmware.com/en/VMware-vSphere/8.0/vsphere-networking/GUID-375B45C7-684C-4C51-BA3C-70E48DFABF04.html)) or an [NSX segment](https://docs.vmware.com/en/VMware-NSX/4.1/administration/GUID-316E5027-E588-455C-88AD-A7DA930A4F0B.html). VMs connected to this network should be able to obtain IP address via DHCP.
* A datastore that can be accessed by all the hosts in the vSphere cluster.

Another way to prepare the vSphere environment is with VMware Cloud Foundation (VCF). VCF is a unified software-defined datacenter (SDDC) platform that seamlessly integrates vSphere, vSAN, and NSX into a natively integrated stack, delivering enterprise-ready cloud infrastructure for both private and public cloud environments. If you are using VCF, you can refer to the VCF documentation to  [create workload domains](https://docs.vmware.com/en/VMware-Cloud-Foundation/5.0/vcf-admin/GUID-3A478CF8-AFF8-43D9-9635-4E40A0E372AD.html) for running Ray Clusters. A VCF workload domain comprises one or more vSphere clusters, shared storage like vSAN, and a software-defined network managed by NSX. You can also [create NSX Edge Clusters using VCF](https://docs.vmware.com/en/VMware-Cloud-Foundation/5.0/vcf-admin/GUID-D17D0274-7764-43BD-8252-D9333CA7415A.html) and create segment for Ray VMs network.

## Prepare the frozen VM

The vSphere Ray cluster launcher requires the vSphere environment to have a VM in a frozen state for deploying a Ray cluster. This VM has all the dependencies installed and is later used to rapidly create head and worker nodes by VMware's [instant clone](https://docs.vmware.com/en/VMware-vSphere/8.0/vsphere-vm-administration/GUID-853B1E2B-76CE-4240-A654-3806912820EB.html) technology. The details of the Ray cluster provisioning process using frozen VM can be found in this [Ray on vSphere architecture document](https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/_private/vsphere/ARCHITECTURE.md). 

You can follow the vm-packer-for-ray's [document](https://github.com/vmware-ai-labs/vm-packer-for-ray/blob/main/README.md) to use Packer to create and set up the frozen VM, or a set of frozen VMs in which each one will be hosted on a distinct ESXi host in the vSphere cluster. By default, Ray clusters' head and worker node VMs will be placed in the same resource pool as the frozen VM. When building and deploying the frozen VM, there are a couple of things to note:

* The VM's network adapter should be connected to the port group or NSX segment configured in the above section. And the `Connect At Power On` check box should be selected.
* After the frozen VM is built, a private key file (`ray-bootstrap-key.pem`) and a public key file (`ray_bootstrap_public_key.key`) will be generated under the HOME directory of the current user. If you want to deploy Ray clusters from another machine, these files should be copied to that machine's HOME directory to be picked up by the vSphere cluster launcher.
* An OVF will be generated in the content library. If you want to deploy Ray clusters in other vSphere deployments, you can use the content library's [publish and subscribe](https://docs.vmware.com/en/VMware-vSphere/8.0/vsphere-vm-administration/GUID-254B2CE8-20A8-43F0-90E8-3F6776C2C896.html) feature to sync the frozen VM's template to another vSphere environment. Then you can leverage Ray Cluster Launcher to help you create a single frozen VM or multiple frozen VMs firstly, then help you create the Ray cluster, check the [document](https://docs.ray.io/en/latest/cluster/vms/references/ray-cluster-configuration.html?highlight=yaml#vsphere-config-frozen-vm) for how to compose the yaml file to help to deploy the frozen VM(s) from an OVF template. 

## Install Ray cluster launcher

The Ray cluster launcher is part of the `ray` CLI. Use the CLI to start, stop and attach to a running ray cluster using commands such as `ray up`, `ray down` and `ray attach`. You can use pip to install the ray CLI with cluster launcher support. Follow [the Ray installation documentation](installation) for more detailed instructions.

```bash
# install ray
pip install -U ray[default]
```

## Install VMware vSphere Automation SDK for Python

Next, install the VMware vSphere Automation SDK for Python.

```bash
# Install the VMware vSphere Automation SDK for Python.
pip install 'git+https://github.com/vmware/vsphere-automation-sdk-python.git'
```

You can append a version tag to install a specific version.
```bash
# Install the v8.0.1.0 version of the SDK.
pip install 'git+https://github.com/vmware/vsphere-automation-sdk-python.git@v8.0.1.0'
```

## Start Ray with the Ray cluster launcher

Once the vSphere Automation SDK is installed, you should be ready to launch your cluster using the cluster launcher. The provided [cluster config file](https://raw.githubusercontent.com/ray-project/ray/master/python/ray/autoscaler/vsphere/example-full.yaml) will create a small cluster with a head node configured to autoscale to up to two workers.

Note that you need to configure your vSphere credentials and vCenter server address either via setting environment variables or adding them to the Ray cluster configuration YAML file.

Test that it works by running the following commands from your local machine:

```bash
# Download the example-full.yaml
wget https://raw.githubusercontent.com/ray-project/ray/master/python/ray/autoscaler/vsphere/example-full.yaml

# Setup vSphere credentials using environment variables
export VSPHERE_SERVER=vcenter-address.example.com
export VSPHERE_USER=foo
export VSPHERE_PASSWORD=bar

# Edit the example-full.yaml to update the frozen VM related configs under vsphere_config. there are 3 options:
# 1. If you have a single frozen VM, set the "name" under "frozen_vm".
# 2. If you have a set of frozen VMs in a resource pool (one VM on each ESXi host), set the "resource_pool" under "frozen_vm".
# 3. If you don't have any existing frozen VM in the vSphere cluster, but you have an OVF template of a frozen VM, set the "library_item" under "frozen_vm". After that, you need to either set the "name" of the to-be-deployed frozen VM, or set the "resource_pool" to point to an existing resource pool for the to-be-deployed frozen VMs for all the ESXi hosts in the vSphere cluster. Also, the "datastore" must be specified.
# Optionally configure the head and worker node resource pool and datastore placement.
# If not configured via environment variables, the vSphere credentials can alternatively be configured in this file.

# vi example-full.yaml

# Create or update the cluster. When the command finishes, it will print
# out the command that can be used to SSH into the cluster head node.
ray up example-full.yaml

# Get a remote screen on the head node.
ray attach example-full.yaml

# Try running a Ray program.
python -c 'import ray; ray.init()'
exit

# Tear down the cluster.
ray down example-full.yaml
```

Congrats, you have started a Ray cluster on vSphere!

## Configure vSAN File Service as persistent storage for Ray AI Libraries

Starting in Ray 2.7, Ray AI Libraries (Train and Tune) will require users to provide a cloud storage or NFS path when running distributed training or tuning jobs. In a vSphere environment with a vSAN datastore, you can utilize the vSAN File Service feature to employ vSAN as a shared persistent storage. You can refer to [this vSAN File Service document](https://docs.vmware.com/en/VMware-vSphere/8.0/vsan-administration/GUID-CA9CF043-9434-454E-86E7-DCA9AD9B0C09.html) to create and configure NFS file shares supported by vSAN. The general steps are as follows:

1. Enable vSAN File Service and configure it with domain information and IP address pools.
2. Create a vSAN file share with NFS as the protocol.
3. View the file share information to get NFS export path.

Once a file share is created, you can mount it into the head and worker node and use the mount path as the `storage_path` for the `RunConfig` parameter in Ray Train and Tune. Please refer to [this example YAML](https://raw.githubusercontent.com/ray-project/ray/master/python/ray/autoscaler/vsphere/example-vsan-file-service.yaml) as a template on how to mount and configure the path. You will need to modify the NFS export path in the `initialization_commands` list and bind the mounted path within the Ray container. In this example, you will need to put `/mnt/shared_storage/experiment_results` as the `storage_path` for `RunConfig`.
