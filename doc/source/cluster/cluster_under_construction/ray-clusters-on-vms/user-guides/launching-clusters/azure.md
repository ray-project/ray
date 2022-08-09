
# Launching Ray Clusters on Azure 

This guide details the steps needed to start a Ray cluster on Azure.

There are two ways to start an Azure Ray cluster.
- Launch through Ray Cluster Launcher.
- Deploy a cluster using Azure portal .


## Using Ray Cluster Launcher 
### Install Ray Cluster Launcher
The Ray Cluster Launcher is part of the `ray` command line tool. It allows you to start, stop and attach to a running ray cluster using commands such as  `ray up`, `ray down` and `ray attach`. You can use pip to install the ray command line tool with cluster launcher support. Follow [install ray](https://docs.ray.io/en/latest/ray-overview/installation.html) for more detailed instructions.

```
# install ray
pip install -U ray[default]
```

### Install and Configure Azure CLI

Next, install the Azure CLI (`pip install -U azure-cli azure-identity`) then login using (`az login`).  

```
# install azure cli.
pip install azure-cli azure-identity

# login into azure, this will redirect you 
# to your web browser.
az login
```

### Start Ray with the Ray Cluster Launcher


The provided [example-full.yaml](https://github.com/ray-project/ray/tree/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/azure/example-full.yaml) cluster config file will create a small cluster with a Standard DS2v3 node (on-demand) configured to autoscale up to two Standard DS2v3 worker nodes ([spot-instances](https://docs.microsoft.com/en-us/azure/virtual-machines/windows/spot-vms)).

Note that you'll need to fill in your [resource group](https://github.com/ray-project/ray/blob/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/azure/example-full.yaml#L42) and [location](https://github.com/ray-project/ray/blob/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/azure/example-full.yaml#L41) in those templates. You also need set subscription to use from the command line `az account set -s <subscription_id>` or by filling the [subscription_id](https://github.com/ray-project/ray/blob/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/azure/example-full.yaml#L44) in the cluster config.



Test that it works by running the following commands from your local machine:

```
# Download the example-full.yaml
wget https://raw.githubusercontent.com/ray-project/ray/master/python/ray/autoscaler/azure/example-full.yaml

# Update the example-full.yaml to update resource group, location, and subscription_id.
# vi example-full.yaml

# Create or update the cluster. When the command finishes, it will print
# out the command that can be used to SSH into the cluster head node.
ray up example-full.yaml

# Get a remote screen on the head node.
ray attach example-full.yaml
# Try running a Ray program.

# Tear down the cluster.
ray down example-full.yaml
```

Congrats, you have started a Ray cluster on Azure!

## Using Azure portal 

Alternatively, you can deploy a cluster using Azure portal directly. Please note that autoscaling is done using Azure VM Scale Sets and not through the Ray autoscaler. This will deploy [Azure Data Science VMs (DSVM)](https://azure.microsoft.com/en-us/services/virtual-machines/data-science-virtual-machines/) for both the head node and the auto-scalable cluster managed by [Azure Virtual Machine Scale Sets](https://azure.microsoft.com/en-us/services/virtual-machine-scale-sets/).
The head node conveniently exposes both SSH as well as JupyterLab.



Once the template is successfully deployed the deployment Outputs page provides the ssh command to connect and the link to the JupyterHub on the head node (username/password as specified on the template input).
Use the following code in a Jupyter notebook (using the conda environment specified in the template input, py38_tensorflow by default) to connect to the Ray cluster.

```
import ray; ray.init()
```

Under the hood, the [azure-init.sh](https://github.com/ray-project/ray/blob/master/doc/azure/azure-init.sh) script is executed and performs the following actions:

1. Activates one of the conda environments available on DSVM
2. Installs Ray and any other user-specified dependencies
3. Sets up a systemd task (``/lib/systemd/system/ray.service``) to start Ray in head or worker mode
