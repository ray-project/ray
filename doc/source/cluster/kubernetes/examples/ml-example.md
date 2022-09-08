(kuberay-ml-example)=

# Ray AIR XGBoostTrainer on Kubernetes

:::{note}
To learn the basics of Ray on Kubernetes, we recommend taking a look
at the {ref}`introductory guide <kuberay-quickstart>` first.
:::


In this guide, we show you how to run a sample Ray machine learning
workload on Kubernetes infrastructure.

We will run Ray's {ref}`XGBoost training benchmark <xgboost-benchmark>` with a 100 gigabyte training set.
To learn more about using Ray's XGBoostTrainer, check out {ref}`the XGBoostTrainer documentation <train-gbdt-guide>`.

## Kubernetes infrastructure setup

If you are new to Kubernetes and you are planning to deploy Ray workloads on a managed
Kubernetes service, we recommend taking a look at this {ref}`introductory guide <kuberay-k8s-setup>`
first.

For the workload in this guide, it is recommended to use a pool or group of Kubernetes nodes
with the following properties:
- 10 nodes total
- A capacity of 16 CPU and 64 Gi memory per node. For the major cloud providers, suitable instance types include
    * m5.4xlarge (Amazon Web Services)
    * Standard_D5_v2 (Azure)
    * e2-standard-16 (Google Cloud)
- Each node should be configured with 1000 gigabytes of disk space (to store the training set).

```{admonition} Optional: Set up an autoscaling node pool
**If you would like to try running the workload with autoscaling enabled**, use an autoscaling
node group or pool with a 1 node minimum and a 10 node maximum.
The 1 static node will be used to run the Ray head pod. This node may also host the KubeRay
operator and Kubernetes system components. After the workload is submitted, 9 additional nodes will
scale up to accommodate Ray worker pods. These nodes will scale back down after the workload is complete.
```

## Deploy the KubeRay operator

Once you have set up your Kubernetes cluster, deploy the KubeRay operator.
Refer to the {ref}`Getting Started guide <kuberay-operator-deploy>`
for instructions on this step.

## Deploy a Ray cluster

Now we're ready to deploy the Ray cluster that will execute our workload.

:::{tip}
The Ray cluster we'll deploy is configured such that one Ray pod will be scheduled
per 16-CPU Kubernetes node. The pattern of one Ray pod per Kubernetes node is encouraged, but not required.
Broadly speaking, it is more efficient to use a few large Ray pods than many small ones.
:::

We recommend taking a look at the [config file][ConfigLink] applied in the following command.
```shell
kubectl apply -f https://raw.githubusercontent.com/ray-project/ray/releases/2.0.0/doc/source/cluster/kubernetes/configs/xgboost-benchmark.yaml
```

A Ray head pod and 9 Ray worker pods will be created.


```{admonition} Optional: Deploying an autoscaling Ray cluster
If you've set up an autoscaling node group or pool, you may wish to deploy
an autoscaling cluster by applying the config [xgboost-benchmark-autoscaler.yaml][ConfigLinkAutoscaling].
One Ray head pod will be created. Once the workload starts, the Ray autoscaler will trigger
creation of Ray worker pods. Kubernetes autoscaling will then create nodes to place the Ray pods.
```

## Run the workload

To observe the startup progress of the Ray head pod, run the following command.

```shell
# If you're on MacOS, first `brew install watch`.
watch -n 1 kubectl get pod
```

Once the Ray head pod enters `Running` state, we are ready to execute the XGBoost workload.
We will use {ref}`Ray Job Submission <jobs-overview>` to kick off the workload.

### Connect to the cluster.

First, we connect to the Job server. Run the following blocking command
in a separate shell.
```shell
kubectl port-forward service/raycluster-xgboost-benchmark-head-svc 8265:8265
```

### Submit the workload.

We'll use the {ref}`Ray Job Python SDK <ray-job-sdk>` to submit the XGBoost workload.

```{literalinclude} /cluster/doc_code/xgboost_submit.py
:language: python
```

To submit the workload, run the above Python script.
The script is available [in the Ray repository][XGBSubmit].

```shell
# Download the above script.
curl https://raw.githubusercontent.com/ray-project/ray/releases/2.0.0/doc/source/cluster/doc_code/xgboost_submit.py -o xgboost_submit.py
# Run the script.
python xgboost_submit.py
```

### Observe progress.

The benchmark may take up to 30 minutes to run.
Use the following tools to observe its progress.

#### Job logs

To follow the job's logs, use the command printed by the above submission script.
```shell
# Substitute the Ray Job's submission id.
ray job logs 'raysubmit_xxxxxxxxxxxxxxxx' --follow
```

#### Kubectl

Observe the pods in your cluster with
```shell
# If you're on MacOS, first `brew install watch`.
watch -n 1 kubectl get pod
```

#### Ray Dashboard

View `localhost:8265` in your browser to access the Ray Dashboard.

#### Ray Status

Observe autoscaling status and Ray resource usage with
```shell
# Substitute the name of your Ray cluster's head pod.
watch -n 1 kubectl exec -it raycluster-xgboost-benchmark-head-xxxxx -- ray status
```

:::{note}
Under some circumstances and for certain cloud providers,
the K8s API server may become briefly unavailable during Kuberentes
cluster resizing events.

Don't worry if that happens -- the Ray workload should be uninterrupted.
For the example in this guide, wait until the API server is back up, restart the port-forwarding process,
and re-run the job log command.
:::

### Job completion

#### Benchmark results

Once the benchmark is complete, the job log will display the results:

```
Results: {'training_time': 1338.488839321999, 'prediction_time': 403.36653568099973}
```

The performance of the benchmark is sensitive to the underlying cloud infrastructure --
you might not match {ref}`the numbers quoted in the benchmark docs <xgboost-benchmark>`.

#### Model parameters
The file `model.json` in the Ray head pod contains the parameters for the trained model.
Other result data will be available in the directory `ray_results` in the head pod.
Refer to the {ref}`the XGBoostTrainer documentation <train-gbdt-guide>` for details.

```{admonition} Scale-down
If autoscaling is enabled, Ray worker pods will scale down after 60 seconds.
After the Ray worker pods are gone, your Kubernetes infrastructure should scale down
the nodes that hosted these pods.
```

#### Clean-up
Delete your Ray cluster with the following command:
```shell
kubectl delete raycluster raycluster-xgboost-benchmark
```
If you're on a public cloud, don't forget to clean up the underlying
node group and/or Kubernetes cluster.

[ConfigLink]:https://raw.githubusercontent.com/ray-project/ray/releases/2.0.0/doc/source/cluster/kubernetes/configs/xgboost-benchmark.yaml
[ConfigLinkAutoscaling]: https://raw.githubusercontent.com/ray-project/ray/releases/2.0.0/doc/source/cluster/kubernetes/configs/xgboost-benchmark-autoscaler.yaml
[XGBSubmit]: https://github.com/ray-project/ray/blob/releases/2.0.0/doc/source/cluster/doc_code/xgboost_submit.py
