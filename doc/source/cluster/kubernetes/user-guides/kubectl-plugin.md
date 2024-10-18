(kubectl-plugin)=

# Use Kubectl Plugin (Alpha Feature)

Today it is incredibly challenging for data scientists and AI researchers unfamiliar with Kubernetes to start using Ray on Kubernetes. However, running Ray on Kubernetes is advantageous or necessary in certain environments.

Therefore, starting from KubeRay v1.2.2, we have introduced a `kubectl ray` plugin to simplify the process of running Ray on Kubernetes. The plugin is designed to help users who are not familiar with Kubernetes to run Ray on Kubernetes with ease.

## Installation

Please follow the instructions in the [KubeRay Kubectl Plugin](https://github.com/ray-project/kuberay/tree/master/kubectl-plugin) to install the plugin.

Currently, the recommended way to install the plugin is to download the binary from the [release page](https://github.com/ray-project/kuberay/releases) and add it to your PATH.

For example, to install plugin version 1.2.2 on Linux amd64:

```shell
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
$ kubectl ray log ray-cluster-kuberay

No output directory specified, creating dir under current directory using cluster name.
Downloading file ./ for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./old/ for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./gcs_server.out for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./gcs_server.err for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./events/ for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./events/event_GCS.log for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./events/event_AUTOSCALER.log for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./events/event_RAYLET.log for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./debug_state_gcs.txt for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./monitor.out for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./monitor.err for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./ray_client_server.out for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./ray_client_server.err for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./dashboard.err for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./dashboard.log for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./monitor.log for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./raylet.out for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./raylet.err for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./log_monitor.err for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./debug_state.txt for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./log_monitor.log for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./dashboard_agent.log for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./agent-424238335.out for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./agent-424238335.err for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./runtime_env_agent.out for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./runtime_env_agent.err for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./runtime_env_agent.log for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./nsight/ for Ray Head ray-cluster-kuberay-head-qkrv8
```
