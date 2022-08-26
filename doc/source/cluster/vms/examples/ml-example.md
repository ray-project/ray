(clusters-vm-ml-example)=

# Ray AIR XGBoostTrainer on VMs

:::{note}
To learn the basics of Ray on VMs, we recommend taking a look
at the {ref}`introductory guide <vm-cluster-quick-start>` first.
:::


In this guide, we show you how to run a sample Ray machine learning
workload on AWS. The similar steps can be used to deploy on GCP or Azure as well.

We will run Ray's {ref}`XGBoost training benchmark <xgboost-benchmark>` with a 100 gigabyte training set.
To learn more about using Ray's XGBoostTrainer, check out {ref}`the XGBoostTrainer documentation <train-gbdt-guide>`.

## VM cluster setup

For the workload in this guide, it is recommended to use the following setup:
- 10 nodes total
- A capacity of 16 CPU and 64 Gi memory per node. For the major cloud providers, suitable instance types include
    * m5.4xlarge (Amazon Web Services)
    * Standard_D5_v2 (Azure)
    * e2-standard-16 (Google Cloud)
- Each node should be configured with 1000 gigabytes of disk space (to store the training set).

The corresponding cluster configuration file is as follows:

```{literalinclude} ../configs/xgboost-benchmark.yaml
:language: yaml
```

```{admonition} Optional: Set up an autoscaling cluster
**If you would like to try running the workload with autoscaling enabled**,
change ``min_workers`` of worker nodes to 0.
After the workload is submitted, 9 workers nodes will
scale up to accommodate the workload. These nodes will scale back down after the workload is complete.
```

## Deploy a Ray cluster

Now we're ready to deploy the Ray cluster with the configuration that's defined above.
Before running the command, make sure your aws credentials are configured correctly.

```shell
ray up -y cluster.yaml
```

A Ray head node and 9 Ray worker nodes will be created.

## Run the workload

We will use {ref}`Ray Job Submission <jobs-overview>` to kick off the workload.

### Connect to the cluster

First, we connect to the Job server. Run the following blocking command
in a separate shell.
```shell
ray dashboard cluster.yaml
```
This will forward remote port 8265 to port 8265 on localhost.

### Submit the workload

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

### Observe progress

The benchmark may take up to 30 minutes to run.
Use the following tools to observe its progress.

#### Job logs

To follow the job's logs, use the command printed by the above submission script.
```shell
# Subsitute the Ray Job's submission id.
ray job logs 'raysubmit_xxxxxxxxxxxxxxxx' --address="http://localhost:8265" --follow
```

#### Ray Dashboard

View `localhost:8265` in your browser to access the Ray Dashboard.

#### Ray Status

Observe autoscaling status and Ray resource usage with
```shell
ray exec cluster.yaml 'ray status'
```

### Job completion

#### Benchmark results

Once the benchmark is complete, the job log will display the results:

```
Results: {'training_time': 1338.488839321999, 'prediction_time': 403.36653568099973}
```

The performance of the benchmark is sensitive to the underlying cloud infrastructure --
you might not match {ref}`the numbers quoted in the benchmark docs <xgboost-benchmark>`.

#### Model parameters
The file `model.json` in the Ray head node contains the parameters for the trained model.
Other result data will be available in the directory `ray_results` in the head node.
Refer to the {ref}`XGBoostTrainer documentation <train-gbdt-guide>` for details.

```{admonition} Scale-down
If autoscaling is enabled, Ray worker nodes will scale down after the specified idle timeout.
```

#### Clean-up
Delete your Ray cluster with the following command:
```shell
ray down -y cluster.yaml
```

[XGBSubmit]: https://github.com/ray-project/ray/blob/releases/2.0.0/doc/source/cluster/doc_code/xgboost_submit.py
