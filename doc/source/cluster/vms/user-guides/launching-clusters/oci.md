---
myst:
  html_meta:
    description: "Launch a Ray cluster on Oracle Cloud Infrastructure (OCI) with the cluster launcher, covering the OCI SDK, credentials, networking, instance principals and GPU shapes."
---

# Launching Ray Clusters on OCI

This guide details the steps needed to start a Ray cluster on Oracle Cloud Infrastructure (OCI).

To start an OCI Ray cluster, you should use the Ray cluster launcher with the OCI Python SDK.

```{note}
The OCI cluster launcher is community-maintained. Please open a GitHub issue with the `oci` keyword in the title if you run into problems.
```

## Install Ray cluster launcher

The Ray cluster launcher is part of the `ray` CLI. Use the CLI to start, stop and attach to a running ray cluster using commands such as `ray up`, `ray down` and `ray attach`. You can use pip to install the ray CLI with cluster launcher support. Follow [the Ray installation documentation](installation) for more detailed instructions.

```bash
# install ray
pip install -U ray[default]
```

## Install and configure the OCI Python SDK

Next, install the OCI Python SDK and configure your credentials following [the OCI SDK configuration guide](https://docs.oracle.com/iaas/Content/API/Concepts/sdkconfig.htm). The launcher reads a profile from your OCI config file (`~/.oci/config` by default) and supports both API-key profiles and the session-token profiles created by `oci session authenticate`.

```bash
# install the OCI Python SDK
pip install -U oci

# API key: create ~/.oci/config interactively
oci setup config

# or, session token (browser login, valid for one hour, refresh with `oci session refresh`)
oci session authenticate --profile-name DEFAULT --region us-ashburn-1
```

The profile needs permission to manage instances, use the virtual network and read images in the compartment you launch into. When `ray up` also has to create the default network and IAM resources described below, it additionally needs to manage `virtual-network-family` in the compartment, and `dynamic-groups` and `policies` in the tenancy.

## Start Ray with the Ray cluster launcher

Once the OCI SDK is configured to manage resources in your tenancy, you should be ready to launch your cluster using the cluster launcher. The provided [cluster config file](https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/oci/example-full.yaml) will create a small cluster with a `VM.Standard.E4.Flex` head node configured to autoscale to up to two `VM.Standard.E4.Flex` workers and one `VM.GPU.A10.1` GPU worker.

Set `provider.region` and `provider.compartment_id` in the config file to your region and the OCID of the compartment in which the nodes should run. Everything else is optional:

* **Networking.** If `provider.subnet_id` is not set, `ray up` creates a VCN named `ray-autoscaler-vcn` (10.77.0.0/16) with a public subnet, an internet gateway, and a security list that allows SSH from anywhere and all traffic inside the VCN. The resources are reused on later runs and are never deleted by `ray down`. Use `vcn_cidr` and `subnet_cidr` to change the address ranges.
* **Head node credentials.** The autoscaler runs on the head node and needs to call OCI APIs. By default (`use_instance_principal: true`) `ray up` creates a dynamic group matching the instances in the compartment and a policy that lets it manage instances, volumes and use the network in that compartment, so the head node authenticates with [instance principals](https://docs.oracle.com/iaas/Content/Identity/Tasks/callingservicesfrominstances.htm). If you are not allowed to create IAM resources, ask an administrator to create them and set `create_iam_resources: false`, or set `use_instance_principal: false` and copy an OCI config file and key to the head node with `file_mounts`.
* **SSH.** If `auth.ssh_private_key` is not set, `ray up` generates `~/.ssh/ray-autoscaler_oci_<region>.pem`. The public key is injected into each instance through the `ssh_authorized_keys` metadata field.
* **Images.** If `node_config.image_id` is not set, the newest Canonical Ubuntu 22.04 platform image compatible with the node's shape is used.

Node types are configured with the snake_case fields of the OCI [`LaunchInstanceDetails`](https://docs.oracle.com/iaas/api/#/en/iaas/latest/datatypes/LaunchInstanceDetails) API model, for example:

```yaml
available_node_types:
    ray.head.default:
        resources: {"CPU": 4}
        node_config:
            shape: VM.Standard.E4.Flex
            shape_config:
                ocpus: 2
                memory_in_gbs: 16
            boot_volume_size_in_gbs: 100
```

CPU and GPU resources are detected from the shape when `resources` is omitted. Ray's node tags are stored as free-form tags on the instances; OCI allows at most 10 free-form tags per instance, so keep user-defined `freeform_tags` to a minimum.

Test that it works by running the following commands from your local machine:

```bash
# Download the example-full.yaml
wget https://raw.githubusercontent.com/ray-project/ray/master/python/ray/autoscaler/oci/example-full.yaml

# Edit provider.region and provider.compartment_id, then create or update the
# cluster. When the command finishes, it will print out the command that can be
# used to SSH into the cluster head node.
ray up example-full.yaml

# Get a remote screen on the head node.
ray attach example-full.yaml

# Try running a Ray program.
python -c 'import ray; ray.init()'
exit

# Tear down the cluster.
ray down example-full.yaml
```

Congrats, you have started a Ray cluster on OCI!

## GPU nodes

OCI's Ubuntu platform images do not include the NVIDIA driver. The `ray.worker.gpu` node type in the example config installs the NVIDIA server driver with DKMS (`nvidia-headless-<series>-server`, so the kernel module always matches the user-space libraries; kernel headers are preinstalled on OCI images) and loads it in its `worker_setup_commands`, without a reboot, so the worker joins the cluster advertising its GPU. Use an image with the driver preinstalled (for example a custom image) and drop those commands if you prefer.

## Notes

* Nodes need a public IP for outbound internet access (package installation) unless the subnet has a NAT gateway. Set `use_internal_ips: true` together with your own `subnet_id` to run a cluster with private IPs only.
* `cache_stopped_nodes` defaults to `false`: OCI keeps billing GPU and dense I/O shapes while they are stopped. Set it to `true` to stop and reuse standard shapes instead of terminating them.
* Workers can be launched as preemptible instances with `preemptible_instance_config` in `node_config`.
