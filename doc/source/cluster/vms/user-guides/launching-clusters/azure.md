
# Launching Ray Clusters on Azure

This guide details the steps needed to start a Ray cluster on Azure.

There are two ways to start an Azure Ray cluster.
- Launch through Ray cluster launcher.
- Deploy a cluster using Azure portal.

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



##### Automatic SSH Key Generation

To connect to the provisioned head node VM, Ray has automatic SSH Key Generation if none are specified in the config. This is the simplest approach and requires no manual key management.

The default configuration in `example-full.yaml` uses automatic key generation:

```yaml
auth:
    ssh_user: ubuntu
    # SSH keys are auto-generated if not specified
    # Uncomment and specify custom paths if you want to use existing keys:
    # ssh_private_key: /path/to/your/key.pem
    # ssh_public_key: /path/to/your/key.pub
```

##### (Optional) Manual SSH Key Configuration

If you prefer to use your own existing SSH keys, uncomment and specify both of the key paths in the `auth` section. 

For example, to use an existing `ed25519` key pair:

```yaml
auth:
    ssh_user: ubuntu
    ssh_private_key: ~/.ssh/id_ed25519
    ssh_public_key: ~/.ssh/id_ed25519.pub
```

Or for RSA keys:

```yaml
auth:
    ssh_user: ubuntu
    ssh_private_key: ~/.ssh/id_rsa
    ssh_public_key: ~/.ssh/id_rsa.pub
```

Both methods inject the public key directly into the VM's `~/.ssh/authorized_keys` via Azure ARM templates.

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
