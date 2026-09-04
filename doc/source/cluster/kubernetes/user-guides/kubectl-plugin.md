---
myst:
  html_meta:
    description: "Use the kubectl ray plugin to simplify RayCluster management and RayJob submission, with Krew installation and examples."
---

(kubectl-plugin)=

# Use kubectl plugin (beta)

Starting from KubeRay v1.3.0, you can use the `kubectl ray` plugin to simplify common workflows when deploying Ray on Kubernetes. If you aren't familiar with Kubernetes, this plugin simplifies running Ray on Kubernetes.

## Installation

See [KubeRay kubectl Plugin](https://github.com/ray-project/kuberay/tree/master/kubectl-plugin) to install the plugin.

Install the KubeRay kubectl plugin using one of the following methods:

- Install using Krew kubectl plugin manager (recommended)
- Download from GitHub releases

```{admonition} Plugin since 1.4.0 may be incompatible with KubeRay before 1.4.0
:class: warning
Plugin versions since 1.4.0 may be incompatible with KubeRay versions before 1.4.0.
Try to use the same plugin and KubeRay versions.
```

### Install using the Krew kubectl plugin manager (recommended)

1. Install [Krew](https://krew.sigs.k8s.io/docs/user-guide/setup/install/).
2. Download the plugin list by running `kubectl krew update`.
3. Install the plugin by running `kubectl krew install ray`.

### Download from GitHub releases

Go to the [releases page](https://github.com/ray-project/kuberay/releases) and download the binary for your platform.

For example, to install kubectl plugin version 1.7.0 on Linux amd64:

```bash
curl -LO https://github.com/ray-project/kuberay/releases/download/v1.7.0/kubectl-ray_v1.7.0_linux_amd64.tar.gz
tar -xvf kubectl-ray_v1.7.0_linux_amd64.tar.gz
cp kubectl-ray ~/.local/bin
```

Replace `~/.local/bin` with the directory in your `PATH`.

## Shell Completion

Follow the instructions for installing and enabling [kubectl plugin-completion]

## Usage

After installing the plugin, you can use `kubectl ray --help` to see the available commands and options.

## Examples

Assume that you have installed the KubeRay operator. If not, follow the [KubeRay Operator Installation](kuberay-operator-deploy) to install the latest stable KubeRay operator by Helm repository.

### Example 1: RayCluster Management

The `kubectl ray create cluster` command allows you to create a valid RayCluster without an existing YAML file. The default values are follows (empty values mean unset):

| Parameter                                           | Default                        |
|-----------------------------------------------------|--------------------------------|
| K8s labels                                          |                                |
| K8s annotations                                     |                                |
| ray version                                         | 2.46.0                         |
| ray image                                           | rayproject/ray:\<ray version\> |
| head CPU                                            | 2                              |
| head memory                                         | 4Gi                            |
| head GPU                                            | 0                              |
| head ephemeral storage                              |                                |
| head `ray start` parameters                         |                                |
| head node selectors                                 |                                |
| worker replicas                                     | 1                              |
| worker CPU                                          | 2                              |
| worker memory                                       | 4Gi                            |
| worker GPU                                          | 0                              |
| worker TPU                                          | 0                              |
| worker ephemeral storage                            |                                |
| worker `ray start` parameters                       |                                |
| worker node selectors                               |                                |
| Number of hosts in default worker group per replica | 1                              |
| Autoscaler version (v1 or v2)                       |                                |

```text
$ kubectl ray create cluster raycluster-sample
Created Ray Cluster: raycluster-sample
```

You can override the default values by specifying the flags. For example, to create a RayCluster with 2 workers:

```text
$ kubectl ray create cluster raycluster-sample-2 --worker-replicas 2
Created Ray Cluster: raycluster-sample-2
```

You can also override the default values with a config file. For example, the following config file sets the worker CPU to 3.

```text
$ curl -LO https://raw.githubusercontent.com/ray-project/kuberay/refs/tags/v1.7.0/kubectl-plugin/config/samples/create-cluster.sample.yaml
$ kubectl ray create cluster raycluster-sample-3 --file create-cluster.sample.yaml
Created Ray Cluster: raycluster-sample-3
```

See https://github.com/ray-project/kuberay/blob/v1.7.0/kubectl-plugin/config/samples/create-cluster.complete.yaml for the complete list of parameters that you can set in the config file.

By default it only creates one worker group. You can use `kubectl ray create workergroup` to add additional worker groups to existing RayClusters.

```text
$ kubectl ray create workergroup example-group --ray-cluster raycluster-sample --worker-memory 5Gi
```

You can use `kubectl ray get cluster`, `kubectl ray get workergroup`, and `kubectl ray get node` to get the status of RayClusters, worker groups, and Ray nodes, respectively.

```text
$ kubectl ray get cluster
NAME                  NAMESPACE   DESIRED WORKERS   AVAILABLE WORKERS   CPUS   GPUS   TPUS   MEMORY   AGE
raycluster-sample     default     2                 2                   6      0      0      13Gi     3m56s
raycluster-sample-2   default     2                 2                   6      0      0      12Gi     3m51s

$ kubectl ray get workergroup
NAME            REPLICAS   CPUS   GPUS   TPUS   MEMORY   CLUSTER
default-group   1/1        2      0      0      4Gi      raycluster-sample
example-group   1/1        2      0      0      5Gi      raycluster-sample
default-group   2/2        4      0      0      8Gi      raycluster-sample-2

$ kubectl ray get nodes
NAME                                        CPUS   GPUS   TPUS   MEMORY   CLUSTER               TYPE     WORKER GROUP    AGE
raycluster-sample-default-group-4lb5w       2      0      0      4Gi      raycluster-sample     worker   default-group   3m56s
raycluster-sample-example-group-vnkkc       2      0      0      5Gi      raycluster-sample     worker   example-group   3m56s
raycluster-sample-head-vplcq                2      0      0      4Gi      raycluster-sample     head     headgroup       3m56s
raycluster-sample-2-default-group-74nd4     2      0      0      4Gi      raycluster-sample-2   worker   default-group   3m51s
raycluster-sample-2-default-group-vnkkc     2      0      0      4Gi      raycluster-sample-2   worker   default-group   3m51s
raycluster-sample-2-head-pwsrm              2      0      0      4Gi      raycluster-sample-2   head     headgroup       3m51s
```

You can scale a cluster's worker group like so.

```shell
$ kubectl ray scale cluster raycluster-sample \
  --worker-group default-group \
  --replicas 2
Scaled worker group default-group in Ray cluster raycluster-sample in namespace default from 1 to 2 replicas

# verify the worker group scaled up
$ kubectl ray get workergroup default-group --ray-cluster raycluster-sample
NAME            REPLICAS   CPUS   GPUS   TPUS   MEMORY   CLUSTER
default-group   2/2        4      0      0      8Gi      raycluster-sample
```

The `kubectl ray session` command can forward local ports to Ray resources, allowing users to avoid remembering which ports Ray resources exposes.

```text
$ kubectl ray session raycluster-sample
Forwarding ports to service raycluster-sample-head-svc
Ray Dashboard: http://localhost:8265
Ray Interactive Client: http://localhost:10001
```

And then you can open [http://localhost:8265](http://localhost:8265) in your browser to access the dashboard.

The `kubectl ray log` command can download logs from RayClusters to local directories.

```text
$ kubectl ray log raycluster-sample
No output directory specified, creating dir under current directory using resource name.
Command set to retrieve both head and worker node logs.
Downloading log for Ray Node raycluster-sample-default-group-worker-b2k7h
Downloading log for Ray Node raycluster-sample-example-group-worker-sfdp7
Downloading log for Ray Node raycluster-sample-head-k5pj8
```

It creates a folder named `raycluster-sample` in the current directory containing the logs of the RayCluster.

Use `kubectl ray delete` command to clean up the resources.

```text
$ kubectl ray delete raycluster-sample
$ kubectl ray delete raycluster-sample-2
```

(kubectl-plugin-tpu)=
### Create a TPU worker group

Run these commands on a GKE cluster with TPU nodes. See {ref}`the GKE TPU cluster setup <kuberay-gke-tpu-cluster-setup>` to create the cluster, and {ref}`Use TPUs with KubeRay <kuberay-tpu>` for TPU scheduling on Ray. Use the same flags with `kubectl ray create workergroup`.

When you set `--worker-tpu` to a value greater than 0, set these flags:

- `--worker-node-selectors` with `cloud.google.com/gke-tpu-accelerator` and `cloud.google.com/gke-tpu-topology`
- `--num-of-hosts` equal to the topology chip count divided by `--worker-tpu`

The default `--num-of-hosts` is 1, which matches single-host topologies. The plugin validates these values against the [GKE TPU availability table](https://cloud.google.com/kubernetes-engine/docs/concepts/plan-tpus#availability) and rejects invalid combinations.

#### Create a 2D TPU slice

2D accelerators such as `tpu-v5-lite-podslice` and `tpu-v6e-slice` use topologies of the form `AxB`. Some topologies allow more than one `--worker-tpu` value. `2x4` has 8 chips and allows 4 or 8 TPUs per host, so both `--worker-tpu 8 --num-of-hosts 1` and `--worker-tpu 4 --num-of-hosts 2` are valid.

Create a single-host TPU cluster. `1x1` is 1 chip, so use 1 TPU per host and 1 host:

```text
$ kubectl ray create cluster raycluster-tpu-sample --worker-tpu 1 --worker-node-selectors cloud.google.com/gke-tpu-accelerator=tpu-v5-lite-podslice,cloud.google.com/gke-tpu-topology=1x1
Created Ray Cluster: raycluster-tpu-sample
```

A `4x4` topology has 16 chips. With 4 TPUs per host, set `--num-of-hosts` to 4. The following command fails because `--num-of-hosts` defaults to 1:

```text
$ kubectl ray create cluster raycluster-tpu-sample-2 --worker-tpu 4 --worker-node-selectors cloud.google.com/gke-tpu-accelerator=tpu-v6e-slice,cloud.google.com/gke-tpu-topology=4x4
Error: numOfHosts must be 4 for accelerator "tpu-v6e-slice" with topology "4x4" and 4 TPUs per host, got 1. See https://cloud.google.com/kubernetes-engine/docs/concepts/plan-tpus#availability
```

Set `--num-of-hosts` to 4:

```text
$ kubectl ray create cluster raycluster-tpu-sample-2 --worker-tpu 4 --num-of-hosts 4 --worker-node-selectors cloud.google.com/gke-tpu-accelerator=tpu-v6e-slice,cloud.google.com/gke-tpu-topology=4x4
Created Ray Cluster: raycluster-tpu-sample-2
```

#### Create a 3D TPU slice

v4, v5p, and tpu7x use 3D topologies of the form `AxBxC`. The accelerator values are `tpu-v4-podslice`, `tpu-v5p-slice`, and `tpu7x`. These accelerators always use 4 TPUs per host, so `--worker-tpu` must be 4. Set `--num-of-hosts` to the product of the three dimensions divided by 4. Single-host 3D is `2x2x1`, not `1x1`.

The plugin also rejects invalid 3D shapes. `2x2x1` is a special case and always allowed. For topologies with 64 chips or fewer, each dimension must be even. Above 64 chips, each dimension must be multiples of 4, with `A <= B <= C` - for example, `8x4x4` has 128 chips but fails because `8 > 4`.

Create a 3D multi-host TPU cluster. `2x2x2` has 8 chips, so use 4 TPUs per host and 2 hosts:

```text
$ kubectl ray create cluster raycluster-tpu-sample-3 --worker-tpu 4 --num-of-hosts 2 --worker-node-selectors cloud.google.com/gke-tpu-accelerator=tpu-v4-podslice,cloud.google.com/gke-tpu-topology=2x2x2
Created Ray Cluster: raycluster-tpu-sample-3
```

#### Skip TPU validation

If GKE supports a TPU configuration that the plugin hasn't listed yet, pass `--skip-tpu-validation`. The plugin then skips accelerator, topology, and host-count checks.

```text
$ kubectl ray create cluster raycluster-tpu-sample-4 --worker-tpu 4 --num-of-hosts 4 --worker-node-selectors cloud.google.com/gke-tpu-accelerator=ACCELERATOR,cloud.google.com/gke-tpu-topology=TOPOLOGY --skip-tpu-validation
Created Ray Cluster: raycluster-tpu-sample-4
```

#### Delete the TPU clusters

Use `kubectl ray delete` to remove the clusters:

```text
$ kubectl ray delete raycluster-tpu-sample
$ kubectl ray delete raycluster-tpu-sample-2
$ kubectl ray delete raycluster-tpu-sample-3
$ kubectl ray delete raycluster-tpu-sample-4
```

### Example 2: RayJob Submission

`kubectl ray job submit` is a wrapper around the `ray job submit` command. It can automatically forward the ports to the Ray cluster and submit the job. This command can also provision an ephemeral cluster if the user doesn't provide a RayJob.

Assume that under the current directory, you have a file named `sample_code.py`.

```python
import ray
ray.init(address="auto")

@ray.remote
def f(x):
    return x * x

futures = [f.remote(i) for i in range(4)]
print(ray.get(futures)) # [0, 1, 4, 9]
```

#### Submit a Ray job without a YAML file

You can submit a RayJob without specifying a YAML file. The command generates a RayJob based on the following:

| Parameter                                     | Default                        |
|-----------------------------------------------|--------------------------------|
| ray version                                   | 2.46.0                         |
| ray image                                     | rayproject/ray:\<ray version\> |
| head CPU                                      | 2                              |
| head memory                                   | 4Gi                            |
| head GPU                                      | 0                              |
| worker replicas                               | 1                              |
| worker CPU                                    | 2                              |
| worker memory                                 | 4Gi                            |
| worker GPU                                    | 0                              |
| TTL to clean up RayClsuter after job finished | 0                              |
| Deadline before RayJob reaches Running        | 0                              |

```text
$ kubectl ray job submit --name rayjob-sample --working-dir . -- python sample_code.py
Submitted RayJob rayjob-sample.
Waiting for RayCluster
...
2025-01-06 11:53:34,806	INFO worker.py:1634 -- Connecting to existing Ray cluster at address: 10.12.0.9:6379...
2025-01-06 11:53:34,814	INFO worker.py:1810 -- Connected to Ray cluster. View the dashboard at 10.12.0.9:8265
[0, 1, 4, 9]
2025-01-06 11:53:38,368	SUCC cli.py:63 -- ------------------------------------------
2025-01-06 11:53:38,368	SUCC cli.py:64 -- Job 'raysubmit_9NfCvwcmcyMNFCvX' succeeded
2025-01-06 11:53:38,368	SUCC cli.py:65 -- ------------------------------------------
```

You can also designate a specific RayJob YAML to submit a Ray job.

```text
$ wget https://raw.githubusercontent.com/ray-project/kuberay/refs/heads/master/ray-operator/config/samples/ray-job.interactive-mode.yaml
```

Note that in the RayJob spec, `submissionMode` is `InteractiveMode`.

```text
$ kubectl ray job submit -f ray-job.interactive-mode.yaml --working-dir . -- python sample_code.py
Submitted RayJob rayjob-interactive-mode.
Waiting for RayCluster
...
2025-01-06 12:44:43,542	INFO worker.py:1634 -- Connecting to existing Ray cluster at address: 10.12.0.10:6379...
2025-01-06 12:44:43,551	INFO worker.py:1810 -- Connected to Ray cluster. View the dashboard at 10.12.0.10:8265
[0, 1, 4, 9]
2025-01-06 12:44:47,830	SUCC cli.py:63 -- ------------------------------------------
2025-01-06 12:44:47,830	SUCC cli.py:64 -- Job 'raysubmit_fuBdjGnecFggejhR' succeeded
2025-01-06 12:44:47,830	SUCC cli.py:65 -- ------------------------------------------
```

Use `kubectl ray delete` command to clean up the resources.

```text
$ kubectl ray delete rayjob/rayjob-sample
$ kubectl ray delete rayjob/rayjob-interactive-mode
```

[kubectl plugin-completion]: https://github.com/marckhouzam/kubectl-plugin_completion?tab=readme-ov-file#tldr
