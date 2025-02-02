(kubectl-plugin)=

# Use kubectl plugin (beta)

Starting from KubeRay v1.3, you can use the `kubectl ray` plugin to simplify common workflows when deploying Ray on Kubernetes. If you aren't familiar with Kubernetes, this plugin simplifies running Ray on Kubernetes.

## Installation

See [KubeRay kubectl Plugin](https://github.com/ray-project/kuberay/tree/master/kubectl-plugin) to install the plugin.

Install the KubeRay kubectl plugin using one of the following methods:

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

Most of this example assumes you have a Ray cluster running on Kubernetes. See {ref}`RayCluster Quickstart <kuberay-raycluster-quickstart>` if you don't have a Ray cluster running on Kubernetes.

### Get all Ray clusters

```text
$ kubectl ray get cluster
NAME                             NAMESPACE   DESIRED WORKERS   AVAILABLE WORKERS   CPUS   GPUS   TPUS   MEMORY   AGE
rayjob-sample-raycluster-zwbc6   default     1                 1                   4      0      0      8Gi      71s
sample-cluster                   default     1                 1                   4      0      0      8Gi      21d
```

### Forward local ports to Ray cluster

```text
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

```text
$ kubectl ray logs sample-raycluster-kfhl6
No output directory specified, creating dir under current directory using cluster name.
Command set to retrieve both head and worker node logs.
Downloading log for Ray Node sample-raycluster-kfhl6-head-87xpb
Downloading log for Ray Node sample-raycluster-kfhl6-small-group-worker-54qfm
```

#### Get Ray job logs

You can also access Ray logs with RayJobs and RayServices.

```text
$ kubectl ray logs rayjob/rayjob-interactivemode
No output directory specified, creating dir under current directory using resource name.
Command set to retrieve both head and worker node logs.
Downloading log for Ray Node rayjob-interactivemode-raycluster-qbkr8-head-dwm84
Downloading log for Ray Node rayjob-interactivemode-raycluster-qbkr8-small-grou-worker-hr2jp
```

### Create a Ray cluster

The `ray create cluster` command allows you to create a valid RayCluster without an existing YAML file. The default values are follows:

| Parameter | Default |
| -------- | ---------- |
| ray version | 2.39.0 |
| ray image | rayproject/ray:\<ray version\> |
| head cpu | 2 |
| head memory | 4Gi |
| worker replicas | 1 |
| worker cpu | 2 |
| worker memory | 4Gi |
| worker gpu | 0 |

Currently only one worker group is created.

```text
$ kubectl ray create cluster raycluster-sample
Created Ray Cluster: raycluster-sample
$ kubectl ray get cluster 
NAME                             NAMESPACE   DESIRED WORKERS   AVAILABLE WORKERS   CPUS   GPUS   TPUS   MEMORY   AGE
raycluster-sample                default     1                 1                   4      0      0      8Gi      25s
```

### Submit a Ray job

This is a wrapper around the `ray job submit` command. The plugin can automatically forward the ports to the Ray cluster and submit the job. This command can also provision a ephemeral cluster needed to execute the job if no RayJob is provided.

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

| Parameter | Default |
| -------- | ---------- |
| ray version | 2.39.0 |
| ray image | rayproject/ray:\<ray version\> |
| head cpu | 2 |
| head memory | 4Gi |
| worker replicas | 1 |
| worker cpu | 2 |
| worker memory | 4Gi |
| worker gpu | 0 |

```text
$ kubectl ray job submit --name rayjob-sample --working-dir . -- python sample_code.py
Submitted RayJob rayjob-sample.
Waiting for RayCluster
Checking Cluster Status for cluster rayjob-sample-raycluster-2qgsj...
Cluster is not ready: Cannot determine cluster state
Cluster is not ready: Cannot determine cluster state
Cluster is not ready: Cannot determine cluster state
Cluster is not ready: Cannot determine cluster state
Cluster is not ready: Cannot determine cluster state
Cluster is not ready: Cannot determine cluster state
Cluster is not ready: Cannot determine cluster state
Cluster is not ready: Cannot determine cluster state
Waiting for portforwarding...Port Forwarding service rayjob-sample-raycluster-2qgsj-head-svc
Forwarding from 127.0.0.1:8265 -> 8265
Forwarding from [::1]:8265 -> 8265
Handling connection for 8265
Portforwarding started on http://localhost:8265
Ray command: [ray job submit --address http://localhost:8265 --working-dir . -- python sample_code.py]
Running ray submit job command...
Handling connection for 8265
Handling connection for 8265
Handling connection for 8265
2025-01-06 11:53:32,710	INFO dashboard_sdk.py:338 -- Uploading package gcs://_ray_pkg_bd1a1af41a246cb2.zip.
2025-01-06 11:53:32,714	INFO packaging.py:601 -- Creating a file package for local module '.'.
Handling connection for 8265
Handling connection for 8265
Handling connection for 8265
Handling connection for 8265
Handling connection for 8265
2025-01-06 11:53:32,552	INFO cli.py:39 -- Job submission server address: http://localhost:8265
2025-01-06 11:53:33,629	SUCC cli.py:63 -- -------------------------------------------------------
2025-01-06 11:53:33,630	SUCC cli.py:64 -- Job 'raysubmit_9NfCvwcmcyMNFCvX' submitted successfully
2025-01-06 11:53:33,630	SUCC cli.py:65 -- -------------------------------------------------------
2025-01-06 11:53:33,630	INFO cli.py:289 -- Next steps
2025-01-06 11:53:33,630	INFO cli.py:290 -- Query the logs of the job:
2025-01-06 11:53:33,631	INFO cli.py:292 -- ray job logs raysubmit_9NfCvwcmcyMNFCvX
2025-01-06 11:53:33,631	INFO cli.py:294 -- Query the status of the job:
2025-01-06 11:53:33,631	INFO cli.py:296 -- ray job status raysubmit_9NfCvwcmcyMNFCvX
2025-01-06 11:53:33,631	INFO cli.py:298 -- Request the job to be stopped:
2025-01-06 11:53:33,631	INFO cli.py:300 -- ray job stop raysubmit_9NfCvwcmcyMNFCvX
2025-01-06 11:53:33,786	INFO cli.py:307 -- Tailing logs until the job exits (disable with --no-wait):
2025-01-06 11:53:33,410	INFO job_manager.py:530 -- Runtime env is setting up.
2025-01-06 11:53:34,806	INFO worker.py:1494 -- Using address 10.12.0.9:6379 set in the environment variable RAY_ADDRESS
2025-01-06 11:53:34,806	INFO worker.py:1634 -- Connecting to existing Ray cluster at address: 10.12.0.9:6379...
2025-01-06 11:53:34,814	INFO worker.py:1810 -- Connected to Ray cluster. View the dashboard at 10.12.0.9:8265 
[0, 1, 4, 9]
2025-01-06 11:53:38,368	SUCC cli.py:63 -- ------------------------------------------
2025-01-06 11:53:38,368	SUCC cli.py:64 -- Job 'raysubmit_9NfCvwcmcyMNFCvX' succeeded
2025-01-06 11:53:38,368	SUCC cli.py:65 -- ------------------------------------------
```

#### Submit a Ray job with a RayJob YAML

Users can also designate a specific RayJob YAML to submit a Ray job.

Add the following YAML file `ray-job.interactivemode.yaml`:

```yaml
apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: rayjob-interactivemode
spec:
  submissionMode: InteractiveMode
  rayClusterSpec:
    rayVersion: '2.39.0'
    headGroupSpec:
      rayStartParams:
        dashboard-host: '0.0.0.0'
      template:
        spec:
          containers:
            - name: ray-head
              image: rayproject/ray:2.39.0
              ports:
                - containerPort: 6379
                  name: gcs-server
                - containerPort: 8265
                  name: dashboard
                - containerPort: 10001
                  name: client
              resources:
                limits:
                  cpu: "1"
                requests:
                  cpu: "200m"
    workerGroupSpecs:
      - replicas: 1
        minReplicas: 1
        maxReplicas: 3
        groupName: small-group
        rayStartParams: {}
        template:
          spec:
            containers:
              - name: ray-worker
                image: rayproject/ray:2.39.0
                lifecycle:
                  preStop:
                    exec:
                      command: [ "/bin/sh","-c","ray stop" ]
                resources:
                  limits:
                    cpu: "1"
                  requests:
                    cpu: "200m"
```

Note that in the RayJob spec, `submissionMode` is set to `InteractiveMode`.

```text
$ kubectl ray job submit -f ray-job.interactivemode.yaml --working-dir . -- python sample_code.py
Submitted RayJob rayjob-interactivemode.
Waiting for RayCluster
Checking Cluster Status for cluster rayjob-interactivemode-raycluster-qbkr8...
Cluster is not ready: Cannot determine cluster state
Cluster is not ready: Cannot determine cluster state
Cluster is not ready: Cannot determine cluster state
Cluster is not ready: Cannot determine cluster state
Cluster is not ready: Cannot determine cluster state
Cluster is not ready: Cannot determine cluster state
Cluster is not ready: Cannot determine cluster state
Cluster is not ready: Cannot determine cluster state
Waiting for portforwarding...Port Forwarding service rayjob-interactivemode-raycluster-qbkr8-head-svc
Forwarding from 127.0.0.1:8265 -> 8265
Forwarding from [::1]:8265 -> 8265
Handling connection for 8265
Portforwarding started on http://localhost:8265
Ray command: [ray job submit --address http://localhost:8265 --working-dir . -- python sample_code.py]
Running ray submit job command...
Handling connection for 8265
Handling connection for 8265
Handling connection for 8265
2025-01-06 12:44:41,234	INFO dashboard_sdk.py:338 -- Uploading package gcs://_ray_pkg_3ddba7608d86c45a.zip.
2025-01-06 12:44:41,238	INFO packaging.py:601 -- Creating a file package for local module '.'.
Handling connection for 8265
Handling connection for 8265
Handling connection for 8265
Handling connection for 8265
Handling connection for 8265
2025-01-06 12:44:41,077	INFO cli.py:39 -- Job submission server address: http://localhost:8265
2025-01-06 12:44:42,312	SUCC cli.py:63 -- -------------------------------------------------------
2025-01-06 12:44:42,313	SUCC cli.py:64 -- Job 'raysubmit_fuBdjGnecFggejhR' submitted successfully
2025-01-06 12:44:42,313	SUCC cli.py:65 -- -------------------------------------------------------
2025-01-06 12:44:42,313	INFO cli.py:289 -- Next steps
2025-01-06 12:44:42,313	INFO cli.py:290 -- Query the logs of the job:
2025-01-06 12:44:42,313	INFO cli.py:292 -- ray job logs raysubmit_fuBdjGnecFggejhR
2025-01-06 12:44:42,313	INFO cli.py:294 -- Query the status of the job:
2025-01-06 12:44:42,313	INFO cli.py:296 -- ray job status raysubmit_fuBdjGnecFggejhR
2025-01-06 12:44:42,313	INFO cli.py:298 -- Request the job to be stopped:
2025-01-06 12:44:42,313	INFO cli.py:300 -- ray job stop raysubmit_fuBdjGnecFggejhR
2025-01-06 12:44:42,472	INFO cli.py:307 -- Tailing logs until the job exits (disable with --no-wait):
2025-01-06 12:44:41,931	INFO job_manager.py:530 -- Runtime env is setting up.
2025-01-06 12:44:43,542	INFO worker.py:1494 -- Using address 10.12.0.10:6379 set in the environment variable RAY_ADDRESS
2025-01-06 12:44:43,542	INFO worker.py:1634 -- Connecting to existing Ray cluster at address: 10.12.0.10:6379...
2025-01-06 12:44:43,551	INFO worker.py:1810 -- Connected to Ray cluster. View the dashboard at 10.12.0.10:8265 
[0, 1, 4, 9]
2025-01-06 12:44:47,830	SUCC cli.py:63 -- ------------------------------------------
2025-01-06 12:44:47,830	SUCC cli.py:64 -- Job 'raysubmit_fuBdjGnecFggejhR' succeeded
2025-01-06 12:44:47,830	SUCC cli.py:65 -- ------------------------------------------
```

### Delete a Ray cluster

```text
$ kubectl ray delete raycluster-sample
Are you sure you want to delete raycluster raycluster-sample? (y/yes/n/no) y
Delete raycluster raycluster-sample
```
