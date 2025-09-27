# Launching Ray Clusters on vSphere

This guide details the steps needed to launch a Ray cluster in a vSphere environment.

To start a vSphere Ray cluster, you will use the Ray cluster launcher along with supervisor service (control plane) deployed on vSphere.

## Prepare the vSphere environment

If you don't already have a vSphere deployment, you can learn more about it by reading the [vSphere documentation](https://techdocs.broadcom.com/us/en/vmware-cis/vsphere/vsphere-supervisor/7-0/vsphere-with-tanzu-configuration-and-management-7-0/configuring-and-managing-a-supervisor-cluster/deploy-a-supervisor-with-nsx-networking.html). The vSphere Ray cluster launcher requires vSphere version 9.0 or later, along with the following prerequisites for creating Ray clusters.

* [A vSphere cluster with Workload Control Plane (WCP) enabled ](https://techdocs.broadcom.com/us/en/vmware-cis/vsphere/vsphere-supervisor/7-0/vsphere-with-tanzu-configuration-and-management-7-0/configuring-and-managing-a-supervisor-cluster/deploy-a-supervisor-with-nsx-networking.html)

## Installing supervisor service for Ray on vSphere

Please refer [build and installation guide](https://github-vcf.devops.broadcom.net/vcf/vmray/blob/ivelumani/AIHUB-4311/README.md#generate-carvel-package-for-RayOnVCF-cluster-operator) to install Ray control plane as a superviosr servise on vSphere. The vSphere Ray cluster launcher requires the vSphere environment to have a cotrol plane installed a s a supervisor service for deploying a Ray cluster. This service installs all the k8s CRDs used to rapidly create head and worker nodes. The details of the Ray cluster provisioning process using supervisor service can be found in this [Ray on vSphere architecture document](https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/_private/vsphere/ARCHITECTURE.md).


## Install Ray cluster launcher

The Ray cluster launcher is part of the `ray` CLI. Use the CLI to start, stop and attach to a running ray cluster using commands such as `ray up`, `ray down` and `ray attach`. You can use pip to install the ray CLI with cluster launcher support. Follow [the Ray installation documentation](installation) for more detailed instructions.

```bash
# install ray
pip install -U ray[default]
```

## Start Ray with the Ray cluster launcher

Once the Ray supervisor service is active, you should be ready to launch your cluster using the cluster launcher. The provided [cluster config file](https://raw.githubusercontent.com/ray-project/ray/master/python/ray/autoscaler/vsphere/example-full.yaml) will create a small cluster with a head node configured to autoscale to up to two workers.

Note that you need to configure your vSphere credentials and vCenter server address either via setting environment variables or adding them to the Ray cluster configuration YAML file.

Test that it works by running the following commands from your local machine:

```bash
# Download the example-full.yaml
wget https://raw.githubusercontent.com/ray-project/ray/master/python/ray/autoscaler/vsphere/example-full.yaml

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

Starting in Ray 2.7, Ray AI Libraries (Train and Tune) will require users to provide a cloud storage or NFS path when running distributed training or tuning jobs. In a vSphere environment with a vSAN datastore, you can utilize the vSAN File Service feature to employ vSAN as a shared persistent storage. You can refer to [this vSAN File Service document](https://techdocs.broadcom.com/us/en/vmware-cis/vsan/vsan/8-0/vsan-administration.html) to create and configure NFS file shares supported by vSAN. The general steps are as follows:

1. Enable vSAN File Service and configure it with domain information and IP address pools.
2. Create a vSAN file share with NFS as the protocol.
3. View the file share information to get NFS export path.

Once a file share is created, you can mount it into the head and worker node and use the mount path as the `storage_path` for the `RunConfig` parameter in Ray Train and Tune.
