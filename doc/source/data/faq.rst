.. _datasets_faq:

===
FAQ
===

These are some Frequently Asked Questions that we've seen pop up for Ray Datasets.

.. note::
  For a general conceptual overview of Ray Datasets, see our
  :ref:`Key Concepts docs <data_key_concepts>`.

If you still have questions after reading this FAQ,  please reach out on
`our Discourse <https://discuss.ray.io/>`__!

.. contents::
    :local:
    :depth: 2


What problems does Ray Datasets solve?
======================================

Ray Datasets aims to solve the problems of slow, resource-inefficient, unscalable data
loading and preprocessing pipelines for two core uses cases:

1. **Model training:** resulting in poor training throughput and low GPU utilization as
   the trainers are bottlenecked on preprocessing and data loading.
2. **Batch inference:** resulting in poor batch inference throughput and low GPU
   utilization.

In order to solve these problems without sacrificing usability, Ray Datasets simplifies
parallel and pipelined data processing on Ray, providing a higher-level API while
internally handling data batching, task parallelism and pipelining, and memory
management.

Who is using Ray Datasets?
==========================

To give an idea of Datasets use cases, we list a few notable users running Datasets
integrations in production below:

* Predibase is using Ray Datasets for ML ingest and batch inference in their OSS
  declarative ML framework, `Ludwig <https://github.com/ludwig-ai/ludwig>`__, and
  internally in their `AutoML product <https://predibase.com/>`__.
* Amazon is using Ray Datasets for large-scale I/O in their scalable data catalog,
  `DeltaCAT <https://github.com/ray-project/deltacat>`__.
* Shopify is using Ray Datasets for ML ingest and batch inference in their ML platform,
  `Merlin <https://shopify.engineering/merlin-shopify-machine-learning-platform>`__.
* Ray Datasets is used as the data processing engine for the 
  `Ray-based Apache Beam runner <https://github.com/ray-project/ray_beam_runner>`__.
* Ray Datasets is used as the preprocessing and batch inference engine for
  :ref:`Ray AIR <air>`.


If you're using Ray Datasets, please let us know about your experience on the
`Slack <https://forms.gle/9TSdDYUgxYs8SA9e8>`__  or
`Discourse <https://discuss.ray.io/>`__; we'd love to hear from you!

What should I use Ray Datasets for?
===================================

Ray Datasets is the standard way to load, process, and exchange data in Ray libraries
and applications, with a particular emphasis on ease-of-use, performance, and
scalability in both data size and cluster size. Within that, Datasets is designed for
two core uses cases:

* **ML (training) ingest:** Loading, preprocessing, and ingesting data into one or more
  (possibly distributed) model trainers.
* **Batch inference:** Loading, preprocessing, and performing parallel batch
  inference on data.

We have designed the Datasets APIs, data model, execution model, and
integrations with these use cases in mind, and have captured these use cases in
large-scale nightly tests to ensure that we're hitting our scalability, performance,
and efficiency marks for these use cases.

See our :ref:`ML preprocessing feature guide <datasets-ml-preprocessing>` for more
information on this positioning.

What should I not use Ray Datasets for?
=======================================

Ray Datasets is not meant to be used for generic ETL pipelines (like Spark) or
scalable data science (like Dask, Modin, or Mars). However, each of these frameworks
are :ref:`runnable on Ray <data_integrations>`, and Datasets integrates tightly with
these frameworks, allowing for efficient exchange of distributed data partitions often
with zero-copy. Check out the
:ref:`dataset creation feature guide <dataset_from_in_memory_data_distributed>` to learn
more about these integrations.

Datasets is specifically targeting
the ML ingest and batch inference use cases, with focus on data loading and last-mile
preprocessing for ML pipelines. For more information on this distinction, what we
mean by last-mile preprocessing, and how Ray Datasets fits into a larger ML pipeline
picture, please see our :ref:`ML preprocessing feature guide <datasets-ml-preprocessing>`.

For data loading for training, how does Ray Datasets compare to other solutions?
================================================================================

There are several ML framework-specific and general solutions for loading data into
model trainers. Below, we summarize some advantages Datasets offers over these more
specific ingest frameworks.

Torch datasets (and data loaders)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* **Framework-agnostic:** Datasets is framework-agnostic and portable between different
  distributed training frameworks, while
  `Torch datasets <https://pytorch.org/docs/stable/data.html>`__ are specific to Torch.
* **No built-in IO layer:** Torch datasets do not have an I/O layer for common file formats or in-memory exchange
  with other frameworks; users need to bring in other libraries and roll this
  integration themselves.
* **Generic distributed data processing:** Datasets is more general: it can handle
  generic distributed operations, including global per-epoch shuffling,
  which would otherwise have to be implemented by stitching together two separate
  systems. Torch datasets would require such stitching for anything more involved
  than batch-based preprocessing, and does not natively support shuffling across worker
  shards. See our
  `blog post <https://www.anyscale.com/blog/deep-dive-data-ingest-in-a-third-generation-ml-architecture>`__
  on why this shared infrastructure is important for 3rd generation ML architectures.
* **Lower overhead:** Datasets is lower overhead: it supports zero-copy exchange between
  processes, in contrast to the multi-processing-based pipelines of Torch datasets.

TensorFlow datasets
~~~~~~~~~~~~~~~~~~~

* **Framework-agnostic:** Datasets is framework-agnostic and portable between different
  distributed training frameworks, while
  `TensorFlow datasets <https://www.tensorflow.org/api_docs/python/tf/data/Dataset>`__
  is specific to TensorFlow.
* **Unified single-node and distributed:** Datasets unifies single and multi-node training under
  the same abstraction. TensorFlow datasets presents
  `separate concepts <https://www.tensorflow.org/api_docs/python/tf/distribute/DistributedDataset>`__
  for distributed data loading and prevents code from being seamlessly scaled to larger
  clusters.
* **Lazy execution:** Datasets executed operations eagerly by default, while TensorFlow
  datasets are lazy by default. The formter provides easier iterative development and
  debuggability, and when needing the optimizations that become available with lazy execution,
  Ray Datasets has a lazy execution mode that you can turn on when productionizing your
  integration.
* **Generic distributed data processing:** Datasets is more general: it can handle
  generic distributed operations, including global per-epoch shuffling,
  which would otherwise have to be implemented by stitching together two separate
  systems. TensorFlow datasets would require such stitching for anything more involved
  than basic preprocessing, and does not natively support full-shuffling across worker
  shards; only file interleaving is supported. See our
  `blog post <https://www.anyscale.com/blog/deep-dive-data-ingest-in-a-third-generation-ml-architecture>`__
  on why this shared infrastructure is important for 3rd generation ML architectures.
* **Lower overhead:** Datasets is lower overhead: it supports zero-copy exchange between
  processes, in contrast to the multi-processing-based pipelines of TensorFlow datasets.

Petastorm
~~~~~~~~~

* **Supported data types:** `Petastorm <https://github.com/uber/petastorm>`__ only supports Parquet data, while
  Ray Datasets supports many file formats.
* **Lower overhead:** Datasets is lower overhead: it supports zero-copy exchange between
  processes, in contrast to the multi-processing-based pipelines used by Petastorm.
* **No data processing:** Petastorm does not expose any data processing APIs.

NVTabular
~~~~~~~~~

* **Supported data types:** `NVTabular <https://github.com/NVIDIA-Merlin/NVTabular>`__ only supports tabular
  (Parquet, CSV, Avro) data, while Ray Datasets supports many other file formats.
* **Lower overhead:** Datasets is lower overhead: it supports zero-copy exchange between
  processes, in contrast to the multi-processing-based pipelines used by Petastorm.
* **Heterogeneous compute:** NVTabular doesn't support mixing heterogeneous resources in dataset transforms (e.g.
  both CPU and GPU transformations), while Ray Datasets supports this.
* **ML-specific ops:** NVTabular has a bunch of great ML-specific preprocessing
  operations; this is currently WIP for Ray Datasets:
  :ref:`Ray AIR preprocessors <air-key-concepts>`.

.. _datasets_streaming_faq:

For batch (offline) inference, why should I use Ray Datasets instead of an actor pool?
======================================================================================

Ray Datasets provides its own autoscaling actor pool via the actor compute strategy for
:meth:`ds.map_batches() <ray.data.Dataset.map_batches>`, allowing you to perform CPU- or
GPU-based batch inference on this actor pool. Using this instead of the
`Ray actor pool <https://github.com/ray-project/ray/blob/b17cbd825fe3fbde4fe9b03c9dd33be2454d4737/python/ray/util/actor_pool.py#L6>`__
has a few advantages:

* Ray Datasets actor pool is autoscaling and supports easy-to-configure task dependency
  prefetching, pipelining data transfer with compute.
* Ray Datasets takes care of orchestrating the tasks, batching the data, and managing
  the memory.
* With :ref:`Ray Datasets pipelining <dataset_pipeline_concept>`, Ray Datasets allows you to
  precisely configure pipelining of preprocessing with batch inference, allowing you to
  easily tweak parallelism vs. pipelining to maximize your GPU utilization.
* Ray Datasets provides a broad and performant I/O layer, which you would otherwise have
  to roll yourself.

How fast is Ray Datasets?
=========================

We're still working on open benchmarks, but we've done some benchmarking on synthetic
data and have helped several users port from solutions using Petastorm, Torch
multi-processing data loader, and TensorFlow datasets that have seen a big training
throughput improvement (4-8x) and model accuracy improvement (due to global per-epoch
shuffling) using Ray Datasets.

Please see our
`recent blog post on Ray Datasets <https://www.anyscale.com/blog/ray-datasets-for-machine-learning-training-and-scoring>`__
for more information on this benchmarking.

Does all of my data need to fit into memory?
============================================

No, with Ray's support for :ref:`spilling objects to disk <object-spilling>`, you only
need to be able to fit your data into memory OR disk. However, keeping your data in
distributed memory may speed up your workload, which can be done on arbitrarily large
datasets by windowing them, creating :ref:`pipelines <dataset_pipeline_concept>`.

How much data can Ray Datasets handle?
======================================

Ray Datasets has been tested at multi-petabyte scale for I/O and multi-terabyte scale for
shuffling, and we're continuously working on improving this scalability. If you have a
very large dataset that you'd like to process and you're running into scalability
issues, please reach out to us on our `Discourse <https://discuss.ray.io/>`__.

How do I get my data into Ray Datasets?
=======================================

Ray Datasets supports creating a ``Dataset`` from local and distributed in-memory data
via integrations with common data libraries, as well as from local and remote storage
systems via our support for many common file formats and storage backends.

Check out our :ref:`feature guide for creating datasets <creating_datasets>` for
details.

How do I do streaming/online data loading and processing?
=========================================================

Streaming data loading and data processing can be accomplished by using
:ref:`DatasetPipelines <dataset_pipeline_concept>`. By windowing a dataset, you can
stream data transformations across subsets of the data, even windowing down to the
reading of each file.

See the :ref:`pipelining feature guide <data_pipeline_usage>` for more information.

When should I use :ref:`pipelining <dataset_pipeline_concept>`?
===============================================================

Pipelining is useful in a few scenarios:

* You have two chained operations using different resources (e.g. CPU and GPU) that you
  want to saturate; this is the case for both ML ingest (CPU-based preprocessing and
  GPU-based training) and batch inference (CPU-based preprocessing and GPU-based batch
  inference).
* You want to do streaming data loading and processing in order to keep the size of the
  working set small; see previous FAQ on
  :ref:`how to do streaming data loading and processing <datasets_streaming_faq>`.
* You want to decrease the time-to-first-batch (latency) for a certain operation at the
  end of your workload. This is the case for training and inference since this prevents
  GPUs from being idle (which is costly), and can be advantageous for some other
  latency-sensitive consumers of datasets.

When should I use global per-epoch shuffling?
=============================================

Background
~~~~~~~~~~

When training a machine learning model, shuffling your training dataset is important in
general in order to ensure that your model isn't overfitting on some unintended pattern
in your data, e.g. sorting on the label column, or time-correlated samples. Per-epoch
shuffling in particular can improve your model's precision gain per epoch by reducing
the likelihood of bad (unrepresentative) batches getting you permanently stuck in local
minima: if you get unlucky and your last few batches have noisy labels that pull your
learned weights in the wrong direction, shuffling before the next epoch lets you bounce
out of such a gradient rut. In the distributed data-parallel training case, the current
status quo solution is typically to have a per-shard in-memory shuffle buffer that you
fill up and pop random batches from, without mixing data across shards between epochs.
Ray Datasets also offers fully global random shuffling via
:meth:`ds.random_shuffle() <ray.data.Dataset.random_shuffle()>`, and doing so on an
epoch-repeated dataset pipeline to provide global per-epoch shuffling is as simple as
``ray.data.read().repeat().random_shuffle_each_window()``. But when should you opt for
global per-epoch shuffling instead of local shuffle buffer shuffling?

How to choose a shuffling policy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Global per-epoch shuffling should only be used if your model is sensitive to the
randomness of the training data. There is
`theoretical foundation <https://arxiv.org/abs/1709.10432>`__ for all
gradient-descent-based model trainers benefiting from improved (global) shuffle quality,
and we've found that this is particular pronounced for tabular data/models in practice.
However, the more global your shuffle is, the expensive the shuffling operation, and
this compounds when doing distributed data-parallel training on a multi-node cluster due
to data transfer costs, and this cost can be prohibitive when using very large datasets.

The best route for determining the best tradeoff between preprocessing time + cost and
per-epoch shuffle quality is to measure the precision gain per training step for your
particular model under different shuffling policies:

* no shuffling,
* local (per-shard) limited-memory shuffle buffer,
* local (per-shard) shuffling,
* windowed (psuedo-global) shuffling, and
* fully global shuffling.

From the perspective of keeping preprocessing time in check, as long as your data
loading + shuffling throughput is higher than your training throughput, your GPU should
be saturated, so we like to recommend users with shuffle-sensitive models to push their
shuffle quality higher until this threshold is hit.

What is Arrow and how does Ray Datasets use it?
===============================================

`Apache Arrow <https://arrow.apache.org/>`__ is a columnar memory format and a
single-node data processing and I/O library that Ray Datasets leverages extensively. You
can think of Ray Datasets as orchestrating distributed processing of Arrow data.

See our :ref:`key concepts <data_key_concepts>` for more information on how Ray Datasets
uses Arrow.

How much performance tuning does Ray Datasets require?
======================================================

Ray Datasets doesn't perform query optimization, so some manual performance
tuning may be necessary depending on your use case and data scale. Please see our
:ref:`performance tuning guide <data_performance_tips>` for more information.

How can I contribute to Ray Datasets?
=====================================

We're always happy to accept external contributions! If you have a question, a feature
request, or want to contibute to Ray Datasets or tell us about your use case, please
reach out to us on `Discourse <https://discuss.ray.io/>`__; if you have a you're
confident that you've found a bug, please open an issue on the
`Ray GitHub repo <https://github.com/ray-project/ray>`__. Please see our
:ref:`contributing guide <getting-involved>` for more information!
