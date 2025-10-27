Comparing Ray Data to other systems
===================================

How does Ray Data compare to other solutions for offline inference?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. dropdown:: Batch Services: AWS Batch, GCP Batch

    Cloud providers such as AWS, GCP, and Azure provide batch services to manage compute infrastructure for you. Each service uses the same process: you provide the code, and the service runs your code on each node in a cluster. However, while infrastructure management is necessary, it is often not enough. These services have limitations, such as a lack of software libraries to address optimized parallelization, efficient data transfer, and easy debugging. These solutions are suitable only for experienced users who can write their own optimized batch inference code.

    Ray Data abstracts away not only the infrastructure management, but also the sharding of your dataset, the parallelization of the inference over these shards, and the transfer of data from storage to CPU to GPU.


.. dropdown:: Online inference solutions: Bento ML, Sagemaker Batch Transform

    Solutions like `Bento ML <https://www.bentoml.com/>`_, `Sagemaker Batch Transform <https://docs.aws.amazon.com/sagemaker/latest/dg/batch-transform.html>`_, or :ref:`Ray Serve <rayserve>` provide APIs to make it easy to write performant inference code and can abstract away infrastructure complexities. But they are designed for online inference rather than offline batch inference, which are two different problems with different sets of requirements. These solutions introduce additional complexity like HTTP, and cannot effectively handle large datasets leading inference service providers like `Bento ML to integrating with Apache Spark <https://www.youtube.com/watch?v=HcT0lZ4U1EM>`_ for offline inference.

    Ray Data is built for offline batch jobs, without all the extra complexities of starting servers or sending HTTP requests.

    For a more detailed performance comparison between Ray Data and Sagemaker Batch Transform, see `Offline Batch Inference: Comparing Ray, Apache Spark, and SageMaker <https://www.anyscale.com/blog/offline-batch-inference-comparing-ray-apache-spark-and-sagemaker>`_.

.. dropdown:: Distributed Data Processing Frameworks: Apache Spark and Daft

    Ray Data handles many of the same batch processing workloads as `Apache Spark <https://spark.apache.org/>`_ and `Daft <https://www.getdaft.io>`_, but with a streaming paradigm that is better suited for GPU workloads for deep learning inference.

    However, Ray Data doesn't have a SQL interface unlike Spark and Daft.

    For a more detailed performance comparison between Ray Data and Apache Spark, see `Offline Batch Inference: Comparing Ray, Apache Spark, and SageMaker <https://www.anyscale.com/blog/offline-batch-inference-comparing-ray-apache-spark-and-sagemaker>`_.



How does Ray Data compare to other solutions for ML training ingest?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. dropdown:: PyTorch Dataset and DataLoader

    * **Framework-agnostic:** Datasets is framework-agnostic and portable between different distributed training frameworks, while `Torch datasets <https://pytorch.org/docs/stable/data.html>`__ are specific to Torch.
    * **No built-in IO layer:** Torch datasets do not have an I/O layer for common file formats or in-memory exchange with other frameworks; users need to bring in other libraries and roll this integration themselves.
    * **Generic distributed data processing:** Datasets is more general: it can handle generic distributed operations, including global per-epoch shuffling, which would otherwise have to be implemented by stitching together two separate systems. Torch datasets would require such stitching for anything more involved than batch-based preprocessing, and does not natively support shuffling across worker shards. See our `blog post <https://www.anyscale.com/blog/deep-dive-data-ingest-in-a-third-generation-ml-architecture>`__ on why this shared infrastructure is important for 3rd generation ML architectures.
    * **Lower overhead:** Datasets is lower overhead: it supports zero-copy exchange between processes, in contrast to the multi-processing-based pipelines of Torch datasets.


.. dropdown:: TensorFlow Dataset

    * **Framework-agnostic:** Datasets is framework-agnostic and portable between different distributed training frameworks, while `TensorFlow datasets <https://www.tensorflow.org/api_docs/python/tf/data/Dataset>`__ is specific to TensorFlow.
    * **Unified single-node and distributed:** Datasets unifies single and multi-node training under the same abstraction. TensorFlow datasets presents `separate concepts <https://www.tensorflow.org/api_docs/python/tf/distribute/DistributedDataset>`__ for distributed data loading and prevents code from being seamlessly scaled to larger clusters.
    * **Generic distributed data processing:** Datasets is more general: it can handle generic distributed operations, including global per-epoch shuffling, which would otherwise have to be implemented by stitching together two separate systems. TensorFlow datasets would require such stitching for anything more involved than basic preprocessing, and does not natively support full-shuffling across worker shards; only file interleaving is supported. See our `blog post <https://www.anyscale.com/blog/deep-dive-data-ingest-in-a-third-generation-ml-architecture>`__ on why this shared infrastructure is important for 3rd generation ML architectures.
    * **Lower overhead:** Datasets is lower overhead: it supports zero-copy exchange between processes, in contrast to the multi-processing-based pipelines of TensorFlow datasets.

.. dropdown:: Petastorm

    * **Supported data types:** `Petastorm <https://github.com/uber/petastorm>`__ only supports Parquet data, while Ray Data supports many file formats.
    * **Lower overhead:** Datasets is lower overhead: it supports zero-copy exchange between processes, in contrast to the multi-processing-based pipelines used by Petastorm.
    * **No data processing:** Petastorm does not expose any data processing APIs.


.. dropdown:: NVTabular

    * **Supported data types:** `NVTabular <https://github.com/NVIDIA-Merlin/NVTabular>`__ only supports tabular (Parquet, CSV, Avro) data, while Ray Data supports many other file formats.
    * **Lower overhead:** Datasets is lower overhead: it supports zero-copy exchange between processes, in contrast to the multi-processing-based pipelines used by NVTabular.
    * **Heterogeneous compute:** NVTabular doesn't support mixing heterogeneous resources in dataset transforms (e.g. both CPU and GPU transformations), while Ray Data supports this.
