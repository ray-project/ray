---
myst:
  html_meta:
    description: "How Ray Data compares to batch services, online-inference tools, and distributed frameworks such as Spark and Daft for offline inference."
---

# Comparing Ray Data to other systems

## How does Ray Data compare to other solutions for offline inference?

:::{dropdown} Batch services: AWS Batch and GCP Batch
Cloud providers such as AWS, GCP, and Azure provide batch services that manage compute infrastructure for you. These services follow a similar model, where you provide the code and the service runs it on each node in a cluster. Infrastructure management is necessary, but it's often not enough. These services have limitations, such as a lack of software libraries for optimized parallelization, efficient data transfer, and easy debugging. They suit you only if you're experienced enough to write your own optimized batch inference code.

Ray Data abstracts away more than infrastructure management. It also abstracts away sharding your dataset, parallelizing inference over the shards, and transferring data from storage to CPU to GPU.
:::

:::{dropdown} Online inference solutions: BentoML and SageMaker Batch Transform
Solutions such as [BentoML](https://www.bentoml.com/), [SageMaker Batch Transform](https://docs.aws.amazon.com/sagemaker/latest/dg/batch-transform.html), or {ref}`Ray Serve <rayserve>` provide APIs that simplify writing performant inference code, and they can abstract away infrastructure complexity. However, they target online inference rather than offline batch inference. Online and offline inference are two different problems with different requirements. These solutions add complexity, such as HTTP, and can't effectively handle large datasets. This limitation has led inference service providers such as [BentoML to integrate with Apache Spark](https://www.youtube.com/watch?v=HcT0lZ4U1EM) for offline inference.

Ray Data targets offline batch jobs and avoids the complexity of starting servers or sending HTTP requests.

For a more detailed performance comparison between Ray Data and SageMaker Batch Transform, see [Offline Batch Inference: Comparing Ray, Apache Spark, and SageMaker](https://www.anyscale.com/blog/offline-batch-inference-comparing-ray-apache-spark-and-sagemaker).
:::

:::{dropdown} Distributed data processing frameworks: Apache Spark and Daft
Ray Data handles many of the same batch processing workloads as [Apache Spark](https://spark.apache.org/) and [Daft](https://www.daft.ai), but it uses a streaming paradigm that's better suited to GPU workloads for deep learning inference.

Unlike Spark and Daft, Ray Data doesn't have a SQL interface.

For a more detailed performance comparison between Ray Data and Apache Spark, see [Offline Batch Inference: Comparing Ray, Apache Spark, and SageMaker](https://www.anyscale.com/blog/offline-batch-inference-comparing-ray-apache-spark-and-sagemaker).
:::

## How does Ray Data compare to other solutions for ML training ingest?

:::{dropdown} PyTorch Dataset and DataLoader
Ray Data differs from PyTorch Dataset and DataLoader in the following ways:

* **Framework-agnostic**: Ray Data is framework-agnostic and portable between different distributed training frameworks, while [Torch datasets](https://pytorch.org/docs/stable/data.html) are specific to Torch.
* **No built-in I/O layer**: Torch datasets don't have an I/O layer for common file formats or in-memory exchange with other frameworks. You need to bring in other libraries and build this integration yourself.
* **Generic distributed data processing**: Ray Data is more general. It can handle generic distributed operations, including global per-epoch shuffling, which would otherwise require stitching together two separate systems. Torch datasets require such stitching for anything more involved than batch-based preprocessing, and they don't natively support shuffling across worker shards. For why this shared infrastructure matters for third-generation ML architectures, see this [Anyscale blog post](https://www.anyscale.com/blog/deep-dive-data-ingest-in-a-third-generation-ml-architecture).
* **Lower overhead**: Ray Data has lower overhead. It supports zero-copy exchange between processes, in contrast to the multiprocessing-based pipelines of Torch datasets.
:::

:::{dropdown} TensorFlow Dataset
Ray Data differs from TensorFlow Dataset in the following ways:

* **Framework-agnostic**: Ray Data is framework-agnostic and portable between different distributed training frameworks, while [TensorFlow datasets](https://www.tensorflow.org/api_docs/python/tf/data/Dataset) are specific to TensorFlow.
* **Unified single-node and distributed**: Ray Data unifies single-node and multi-node training under the same abstraction. TensorFlow datasets present [separate concepts](https://www.tensorflow.org/api_docs/python/tf/distribute/DistributedDataset) for distributed data loading and prevent code from scaling seamlessly to larger clusters.
* **Generic distributed data processing**: Ray Data is more general. It can handle generic distributed operations, including global per-epoch shuffling, which would otherwise require stitching together two separate systems. TensorFlow datasets require such stitching for anything more involved than basic preprocessing, and they don't natively support full shuffling across worker shards. They support only file interleaving. For why this shared infrastructure matters for third-generation ML architectures, see this [Anyscale blog post](https://www.anyscale.com/blog/deep-dive-data-ingest-in-a-third-generation-ml-architecture).
* **Lower overhead**: Ray Data has lower overhead. It supports zero-copy exchange between processes, in contrast to the multiprocessing-based pipelines of TensorFlow datasets.
:::

:::{dropdown} Petastorm
Ray Data differs from Petastorm in the following ways:

* **Supported data types**: [Petastorm](https://github.com/uber/petastorm) supports only Parquet data, while Ray Data supports many file formats.
* **Lower overhead**: Ray Data has lower overhead. It supports zero-copy exchange between processes, in contrast to the multiprocessing-based pipelines that Petastorm uses.
* **No data processing**: Petastorm doesn't expose any data processing APIs.
:::

:::{dropdown} NVTabular
Ray Data differs from NVTabular in the following ways:

* **Supported data types**: [NVTabular](https://github.com/NVIDIA-Merlin/NVTabular) supports only tabular data in Parquet, CSV, and Avro formats, while Ray Data supports many other file formats.
* **Lower overhead**: Ray Data has lower overhead. It supports zero-copy exchange between processes, in contrast to the multiprocessing-based pipelines that NVTabular uses.
* **Heterogeneous compute**: NVTabular doesn't support mixing heterogeneous resources in dataset transforms, such as running CPU and GPU transformations together. Ray Data supports this mix.
:::
