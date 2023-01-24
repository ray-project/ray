---
jupytext:
    text_representation:
        extension: .md
        format_name: myst
kernelspec:
    display_name: Python 3
    language: python
    name: python3
orphan: true
---

(train-pipeline-parallelism-example)=

# Model (Pipeline) Parallelism

This example shows how to run the following PyTorch example using Ray Train: [Training Transformer models using Distributed Data Parallel and Pipeline Parallelism](https://pytorch.org/tutorials/advanced/ddp_pipeline.html).

Additional modifications are made to support:

1. Splitting the model across more than 2 GPUs.
2. Training with replicas on different nodes.

## Setup Code

The fully converted `pipeline_parallelism_example.py` Python script is shown below.

```{eval-rst}
.. literalinclude:: /../../python/ray/train/examples/pipeline_parallelism_example.py
```

## Setup Cluster

Define a `pipeline_parallelism.yaml` Ray cluster config. Specifically, this
cluster will start with one 4-GPU node and can autoscale to add another 4-GPU node.

```yaml
cluster_name: pipeline_parallelism

max_workers: 1

provider:
    type: aws
    region: us-west-2

auth:
    ssh_user: ubuntu

available_node_types:
    4_gpu_node: 
        min_workers: 0
        max_workers: 1
        node_config:
            InstanceType: g4dn.12xlarge
            ImageId: latest_dlami
        resources: {}

head_node_type: 4_gpu_node


setup_commands:
    - pip install -U ray torch torchtext torchdata
```

Now, deploy the cluster.

```bash
ray up -y pipeline_parallelism.yaml
```

## Run Script

In one terminal, set up SSH forwarding to allow us to connect to `localhost`.
```bash
ray attach pipeline_parallelism.yaml -p 10001
```

In another (local) terminal, run the script using this cluster with Ray Client. A few possible configurations are shown below.

```bash
# 2 workers, 2 GPU each (single node)
python python/ray/train/examples/pipeline_parallelism_example.py --address "ray://localhost:10001" -n 2 -g 2
# 2 workers, 4 GPU each (multi node)
python python/ray/train/examples/pipeline_parallelism_example.py --address "ray://localhost:10001" -n 2 -g 4
# 4 workers, 2 GPU each (multi node)
python python/ray/train/examples/pipeline_parallelism_example.py --address "ray://localhost:10001" -n 4 -g 2
```
