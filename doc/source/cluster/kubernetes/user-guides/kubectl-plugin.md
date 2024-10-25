(kubectl-plugin)=

# Use Kubectl Plugin (Alpha Feature)

Starting from KubeRay v1.2.2, we have introduced a `kubectl ray` plugin to simplify common workflows when deploying Ray on Kubernetes. The plugin is designed to help users who are not familiar with Kubernetes to run Ray on Kubernetes with ease.

## Installation

Please follow the instructions in the [KubeRay Kubectl Plugin](https://github.com/ray-project/kuberay/tree/master/kubectl-plugin) to install the plugin.

### Install using Krew kubectl plugin manager (Recommended)

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

Here assume you have a Ray cluster running on Kubernetes. Please see {ref}`RayCluster Quickstart <kuberay-raycluster-quickstart>` if you don't have a Ray cluster running on Kubernetes.

### Get all Ray clusters

```shell
$ kubectl ray cluster get

NAME                  NAMESPACE   DESIRED WORKERS   AVAILABLE WORKERS   CPUS   GPUS   TPUS   MEMORY   AGE
ray-cluster-kuberay   default     1                                     2      0      0      3G       24s
```

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
