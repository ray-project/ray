(kubectl-plugin)=

# Use kubectl Plugin (alpha)

Starting from KubeRay v1.2.2, you can use the `kubectl ray` plugin to simplify common workflows when deploying Ray on Kubernetes. If you aren't familiar with Kubernetes, this plugin simplifies running Ray on Kubernetes.

## Installation

See [KubeRay kubectl Plugin](https://github.com/ray-project/kuberay/tree/master/kubectl-plugin) to install the plugin.

Install the Kuberay kubectl plugin using one of the following methods:

- Install using Krew kubectl plugin manager (recommended)
- Download from GitHub releases

### Install using the Krew kubectl plugin manager (recommended)

1. Install [Krew](https://krew.sigs.k8s.io/docs/user-guide/setup/install/).
2. Download the plugin list by running `kubectl krew update`.
3. Install the plugin by running `kubectl krew install ray`.
4. Run `kubectl ray --help` to verify the installation.

### Download from GitHub releases

Go to the [releases page](https://github.com/ray-project/kuberay/releases) and download the binary for your platform.

For example, to install kubectl plugin version 1.2.2 on Linux amd64:

```bash
curl -LO https://github.com/ray-project/kuberay/releases/download/v1.2.2/kubectl-ray_v1.2.2_linux_amd64.tar.gz
tar -xvf kubectl-ray_v1.2.2_linux_amd64.tar.gz
cp kubectl-ray ~/.local/bin
```

Replace `~/.local/bin` with the directory in your `PATH`.

## Usage

After installing the plugin, you can use `kubectl ray --help` to see the available commands and options.

## Example

This example assumes you have a Ray cluster running on Kubernetes. See {ref}`RayCluster Quickstart <kuberay-raycluster-quickstart>` if you don't have a Ray cluster running on Kubernetes.

### Forward local ports to Ray cluster

```shell
$ kubectl ray session ray-cluster-kuberay

Forwarding ports to service ray-cluster-kuberay-head-svc
Ray Dashboard: http://localhost:8265
Ray Interactive Client: http://localhost:10001

Forwarding from 127.0.0.1:8265 -> 8265
Forwarding from [::1]:8265 -> 8265
Forwarding from 127.0.0.1:10001 -> 10001
Forwarding from [::1]:10001 -> 10001
```

### Get Ray cluster logs

```shell
$ kubectl ray logs rayjob-sample-raycluster-kfhl6
No output directory specified, creating dir under current directory using cluster name.
Command set to retrieve both head and worker node logs.
Downloading log for Ray Node rayjob-sample-raycluster-kfhl6-head-87xpb
Downloading log for Ray Node rayjob-sample-raycluster-kfhl6-small-group-worker-54qfm
```
