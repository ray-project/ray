
# Launching Ray Clusters on Azure

This guide details the steps needed to start a Ray cluster on Azure.

There are two ways to start an Azure Ray cluster.
- Launch through Ray cluster launcher.
- Deploy a cluster using Azure portal.

```{note}
The Azure integration is community-maintained. Please reach out to the integration maintainers on Github if
you run into any problems: gramhagen, eisber, ijrsvt.
```

## Using Ray cluster launcher


### Install Ray cluster launcher

The Ray cluster launcher is part of the `ray` CLI. Use the CLI to start, stop and attach to a running ray cluster using commands such as  `ray up`, `ray down` and `ray attach`. You can use pip to install the ray CLI with cluster launcher support. Follow [the Ray installation documentation](installation) for more detailed instructions.

```bash
# install ray
pip install -U ray[default]
```

### Install and Configure Azure CLI

Next, install the Azure CLI (`pip install -U azure-cli azure-identity`) and login using `az login`.

```bash
# Install packages to use azure CLI.
pip install azure-cli azure-identity

# Login to azure. This will redirect you to your web browser.
az login
```

### Install Azure SDK libraries

Now, install the Azure SDK libraries that enable the Ray cluster launcher to build Azure infrastructure.

```bash
# Install azure SDK libraries.
pip install azure-core azure-mgmt-network azure-mgmt-common azure-mgmt-resource azure-mgmt-compute msrestazure
```

### Start Ray with the Ray cluster launcher

The provided [cluster config file](https://github.com/ray-project/ray/tree/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/azure/example-full.yaml) will create a small cluster with a Standard DS2v3 on-demand head node that is configured to autoscale to up to two Standard DS2v3 [spot-instance](https://docs.microsoft.com/en-us/azure/virtual-machines/windows/spot-vms) worker nodes.

Note that you'll need to fill in your Azure [resource_group](https://github.com/ray-project/ray/blob/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/azure/example-full.yaml#L42) and [location](https://github.com/ray-project/ray/blob/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/azure/example-full.yaml#L41) in those templates. You also need set the subscription to use. You can do this from the command line with `az account set -s <subscription_id>` or by filling in the [subscription_id](https://github.com/ray-project/ray/blob/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/azure/example-full.yaml#L44) in the cluster config file.

#### Download and configure the example configuration

Download the reference example locally:

```bash
# Download the example-full.yaml
wget https://raw.githubusercontent.com/ray-project/ray/master/python/ray/autoscaler/azure/example-full.yaml
```

To connect to the provisioned head node VM, you need to ensure that you properly configure the `auth.ssh_private_key`, `auth.ssh_public_key`, and `file_mounts` configuration values to point to file paths on your local environment that have a valid key pair. By default the configuration assumes `$HOME/.ssh/id_rsa` and `$HOME/.ssh/id_rsa.pub`. If you have a different set of key pair files you want to use (for example a `ed25519` pair), update the `example-full.yaml` configurations to use them.

For example a custom-configured `example-full.yaml` file might look like the following if you're using a `ed25519` key pair:

```sh
$ git diff example-full.yaml 
diff --git a/python/ray/autoscaler/azure/example-full.yaml b/python/ray/autoscaler/azure/example-full.yaml
index b25f1b07f1..c65fb77219 100644
--- a/python/ray/autoscaler/azure/example-full.yaml
+++ b/python/ray/autoscaler/azure/example-full.yaml
@@ -61,9 +61,9 @@ auth:
     ssh_user: ubuntu
     # You must specify paths to matching private and public key pair files.
     # Use `ssh-keygen -t rsa -b 4096` to generate a new ssh key pair.
-    ssh_private_key: ~/.ssh/id_rsa
+    ssh_private_key: ~/.ssh/id_ed25519
     # Changes to this should match what is specified in file_mounts.
-    ssh_public_key: ~/.ssh/id_rsa.pub
+    ssh_public_key: ~/.ssh/id_ed25519.pub
 
 # You can make more specific customization to node configurations can be made using the ARM template azure-vm-template.json file.
 # See this documentation here: https://docs.microsoft.com/en-us/azure/templates/microsoft.compute/2019-03-01/virtualmachines
@@ -128,7 +128,7 @@ head_node_type: ray.head.default
 file_mounts: {
     #    "/path1/on/remote/machine": "/path1/on/local/machine",
     #    "/path2/on/remote/machine": "/path2/on/local/machine",
-    "~/.ssh/id_rsa.pub": "~/.ssh/id_rsa.pub"}
+    "~/.ssh/id_ed25519.pub": "~/.ssh/id_ed25519.pub"}
 
 # Files or directories to copy from the head node to the worker nodes. The format is a
 # list of paths. Ray copies the same path on the head node to the worker node.
 ```

#### Launch the Ray cluster on Azure

```bash
# Create or update the cluster. When the command finishes, it will print
# out the command that can be used to SSH into the cluster head node.
ray up example-full.yaml

# Get a remote screen on the head node.
ray attach example-full.yaml
# Try running a Ray program.

# Tear down the cluster.
ray down example-full.yaml
```

Congratulations, you have started a Ray cluster on Azure!

## Using Azure portal

Alternatively, you can deploy a cluster using Azure portal directly. Please note that autoscaling is done using Azure VM Scale Sets and not through the Ray autoscaler. This will deploy [Azure Data Science VMs (DSVM)](https://azure.microsoft.com/en-us/services/virtual-machines/data-science-virtual-machines/) for both the head node and the auto-scalable cluster managed by [Azure Virtual Machine Scale Sets](https://azure.microsoft.com/en-us/services/virtual-machine-scale-sets/).
The head node conveniently exposes both SSH as well as JupyterLab.



Once the template is successfully deployed the deployment Outputs page provides the ssh command to connect and the link to the JupyterHub on the head node (username/password as specified on the template input).
Use the following code in a Jupyter notebook (using the conda environment specified in the template input, py38_tensorflow by default) to connect to the Ray cluster.

```python
import ray; ray.init()
```

Under the hood, the [azure-init.sh](https://github.com/ray-project/ray/blob/master/doc/azure/azure-init.sh) script is executed and performs the following actions:

1. Activates one of the conda environments available on DSVM
2. Installs Ray and any other user-specified dependencies
3. Sets up a systemd task (``/lib/systemd/system/ray.service``) to start Ray in head or worker mode
