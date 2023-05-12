.. _data_faq:

===
FAQ
===

These are some Frequently Asked Questions that we've seen pop up for Ray Data.

.. note::
  For a general conceptual overview of Ray Data, see our
  :ref:`Key Concepts docs <data_key_concepts>`.

If you still have questions after reading this FAQ,  please reach out on
`our Discourse <https://discuss.ray.io/>`__!

.. contents::
    :local:
    :depth: 2


What problems does Ray Data solve?
======================================

Ray Data aims to solve the problems of slow, resource-inefficient, unscalable data
loading and preprocessing pipelines for two core uses cases:

1. **Model training:** resulting in poor training throughput and low GPU utilization as
   the trainers are bottlenecked on preprocessing and data loading.
2. **Batch inference:** resulting in poor batch inference throughput and low GPU
   utilization.

In order to solve these problems without sacrificing usability, Ray Data simplifies
parallel and pipelined data processing on Ray, providing a higher-level API while
internally handling data batching, task parallelism and pipelining, and memory
management.

Who is using Ray Data?
======================

To give an idea of Ray Data use cases, we list a few notable users running Ray Data
integrations in production below:

* Predibase is using Ray Data for ML ingest and batch inference in their OSS
  declarative ML framework, `Ludwig <https://github.com/ludwig-ai/ludwig>`__, and
  internally in their `AutoML product <https://predibase.com/>`__.
* Amazon is using Ray Data for large-scale I/O in their scalable data catalog,
  `DeltaCAT <https://github.com/ray-project/deltacat>`__.
* Shopify is using Ray Data for ML ingest and batch inference in their ML platform,
  `Merlin <https://shopify.engineering/merlin-shopify-machine-learning-platform>`__.
* Ray Data is used as the data processing engine for the
  `Ray-based Apache Beam runner <https://github.com/ray-project/ray_beam_runner>`__.
* Ray Data is used as the preprocessing and batch inference engine for
  :ref:`Ray AIR <air>`.


If you're using Ray Data, please let us know about your experience on the
`Slack <https://forms.gle/9TSdDYUgxYs8SA9e8>`__  or
`Discourse <https://discuss.ray.io/>`__; we'd love to hear from you!

What should I use Ray Data for?
===============================

Ray Data is the standard way to load, process, and exchange data in Ray libraries
and applications, with a particular emphasis on ease-of-use, performance, and
scalability in both data size and cluster size. Within that, Datastreams is designed for
two core uses cases:

* **ML (training) ingest:** Loading, preprocessing, and ingesting data into one or more
  (possibly distributed) model trainers.
* **Batch inference:** Loading, preprocessing, and performing parallel batch
  inference on data.

We have designed the Datastream APIs, data model, execution model, and
integrations with these use cases in mind, and have captured these use cases in
large-scale nightly tests to ensure that we're hitting our scalability, performance,
and efficiency marks for these use cases.

What should I not use Ray Data for?
===================================

Ray Data is not meant to be used for generic ETL pipelines (like Spark) or
scalable data science (like Dask, Modin, or Mars). However, each of these frameworks
are :ref:`runnable on Ray <data_integrations>`, and Datastreams integrates tightly with
these frameworks, allowing for efficient exchange of distributed data partitions often
with zero-copy. Check out the
:ref:`datastream creation feature guide <datastream_from_in_memory_data_distributed>` to learn
more about these integrations.

Datastreams is specifically targeting
the ML ingest and batch inference use cases, with focus on data loading and last-mile
preprocessing for ML pipelines.

For data loading for training, how does Ray Data compare to other solutions?
================================================================================

There are several ML framework-specific and general solutions for loading data into
model trainers. Below, we summarize some advantages Datastreams offers over these more
specific ingest frameworks.

Torch datasets (and data loaders)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* **Framework-agnostic:** Datastreams is framework-agnostic and portable between different
  distributed training frameworks, while
  `Torch datasets <https://pytorch.org/docs/stable/data.html>`__ are specific to Torch.
* **No built-in IO layer:** Torch datasets do not have an I/O layer for common file formats or in-memory exchange
  with other frameworks; users need to bring in other libraries and roll this
  integration themselves.
* **Generic distributed data processing:** Datastreams is more general: it can handle
  generic distributed operations, including global per-epoch shuffling,
  which would otherwise have to be implemented by stitching together two separate
  systems. Torch datasets would require such stitching for anything more involved
  than batch-based preprocessing, and does not natively support shuffling across worker
  shards. See our
  `blog post <https://www.anyscale.com/blog/deep-dive-data-ingest-in-a-third-generation-ml-architecture>`__
  on why this shared infrastructure is important for 3rd generation ML architectures.
* **Lower overhead:** Datastreams is lower overhead: it supports zero-copy exchange between
  processes, in contrast to the multi-processing-based pipelines of Torch datasets.

TensorFlow datasets
~~~~~~~~~~~~~~~~~~~

* **Framework-agnostic:** Datastreams is framework-agnostic and portable between different
  distributed training frameworks, while
  `TensorFlow datasets <https://www.tensorflow.org/api_docs/python/tf/data/Dataset>`__
  is specific to TensorFlow.
* **Unified single-node and distributed:** Datastreams unifies single and multi-node training under
  the same abstraction. TensorFlow datasets presents
  `separate concepts <https://www.tensorflow.org/api_docs/python/tf/distribute/DistributedDataset>`__
  for distributed data loading and prevents code from being seamlessly scaled to larger
  clusters.
* **Generic distributed data processing:** Datastreams is more general: it can handle
  generic distributed operations, including global per-epoch shuffling,
  which would otherwise have to be implemented by stitching together two separate
  systems. TensorFlow datasets would require such stitching for anything more involved
  than basic preprocessing, and does not natively support full-shuffling across worker
  shards; only file interleaving is supported. See our
  `blog post <https://www.anyscale.com/blog/deep-dive-data-ingest-in-a-third-generation-ml-architecture>`__
  on why this shared infrastructure is important for 3rd generation ML architectures.
* **Lower overhead:** Datastreams is lower overhead: it supports zero-copy exchange between
  processes, in contrast to the multi-processing-based pipelines of TensorFlow datasets.

Petastorm
~~~~~~~~~

* **Supported data types:** `Petastorm <https://github.com/uber/petastorm>`__ only supports Parquet data, while
  Ray Data supports many file formats.
* **Lower overhead:** Datastreams is lower overhead: it supports zero-copy exchange between
  processes, in contrast to the multi-processing-based pipelines used by Petastorm.
* **No data processing:** Petastorm does not expose any data processing APIs.

NVTabular
~~~~~~~~~

* **Supported data types:** `NVTabular <https://github.com/NVIDIA-Merlin/NVTabular>`__ only supports tabular
  (Parquet, CSV, Avro) data, while Ray Data supports many other file formats.
* **Lower overhead:** Datastreams is lower overhead: it supports zero-copy exchange between
  processes, in contrast to the multi-processing-based pipelines used by Petastorm.
* **Heterogeneous compute:** NVTabular doesn't support mixing heterogeneous resources in datastream transforms (e.g.
  both CPU and GPU transformations), while Ray Data supports this.
* **ML-specific ops:** NVTabular has a bunch of great ML-specific preprocessing
  operations; this is currently WIP for Ray Data:
  :ref:`Ray AIR preprocessors <air-key-concepts>`.

.. _streaming_faq:

For batch (offline) inference, why should I use Ray Data instead of an actor pool?
======================================================================================

Ray Data provides its own autoscaling actor pool via the actor compute strategy for
:meth:`ds.map_batches() <ray.data.Datastream.map_batches>`, allowing you to perform CPU- or
GPU-based batch inference on this actor pool. Using this instead of the
`Ray actor pool <https://github.com/ray-project/ray/blob/b17cbd825fe3fbde4fe9b03c9dd33be2454d4737/python/ray/util/actor_pool.py#L6>`__
has a few advantages:

* Ray Data actor pool is autoscaling and supports easy-to-configure task dependency
  prefetching, pipelining data transfer with compute.
* Ray Data takes care of orchestrating the tasks, batching the data, and managing
  the memory.
* Ray Data provides a broad and performant I/O layer, which you would otherwise have
  to roll yourself.

How fast is Ray Data?
=========================

We're still working on open benchmarks, but we've done some benchmarking on synthetic
data and have helped several users port from solutions using Petastorm, Torch
multi-processing data loader, and TensorFlow datasets that have seen a big training
throughput improvement (4-8x) and model accuracy improvement (due to global per-epoch
shuffling) using Ray Data.

Please see this
`blog post on Ray Data <https://www.anyscale.com/blog/ray-data-for-machine-learning-training-and-scoring>`__
for more information on this benchmarking.

The new streaming backend for Ray Data (Datastream) supports throughputs of up to
hundreds of gigabytes per second in a large cluster.

Does all of my data need to fit into memory?
============================================

No, with Ray's support for :ref:`spilling objects to disk <object-spilling>`, you only
need to be able to fit your data into memory OR disk. However, keeping your data in
distributed memory may speed up your workload, which can be done on arbitrarily large
datastreams by windowing them, creating pipelines.

How much data can Ray Data handle?
==================================

Ray Data has been tested at multi-petabyte scale for I/O and multi-terabyte scale for
shuffling, and we're continuously working on improving this scalability. If you have a
very large datastream that you'd like to process and you're running into scalability
issues, please reach out to us on our `Discourse <https://discuss.ray.io/>`__.

How do I get my data into Ray Data?
===================================

Ray Data supports creating a ``Datastream`` from local and distributed in-memory data
via integrations with common data libraries, as well as from local and remote storage
systems via our support for many common file formats and storage backends.

For more details, read :ref:`Loading Data <loading_data>`.

When should I use global per-epoch shuffling?
=============================================

Background
~~~~~~~~~~

When training a machine learning model, shuffling your training datastream is important in
general in order to ensure that your model isn't overfitting on some unintended pattern
in your data, e.g. sorting on the label column, or time-correlated samples. Per-epoch
shuffling in particular can improve your model's precision gain per epoch by reducing
the likelihood of bad (unrepresentative) batches getting you permanently stuck in local
minima: if you get unlucky and your last few batches have noisy labels that pull your
learned weights in the wrong direction, shuffling before the next epoch lets you bounce
out of such a gradient rut. In the distributed data-parallel training case, the current
status quo solution is typically to have a per-shard in-memory shuffle buffer that you
fill up and pop random batches from, without mixing data across shards between epochs.
Ray Data also offers fully global random shuffling via
:meth:`ds.random_shuffle() <ray.data.Datastream.random_shuffle()>`, and doing so on an
epoch-repeated datastream pipeline to provide global per-epoch shuffling is as simple as
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
to data transfer costs, and this cost can be prohibitive when using very large datastreams.

The best route for determining the best tradeoff between preprocessing time + cost and
per-epoch shuffle quality is to measure the precision gain per training step for your
particular model under different shuffling policies:

* no shuffling,
* local (per-shard) limited-memory shuffle buffer,
* local (per-shard) shuffling,
* windowed (pseudo-global) shuffling, and
* fully global shuffling.

From the perspective of keeping preprocessing time in check, as long as your data
loading + shuffling throughput is higher than your training throughput, your GPU should
be saturated, so we like to recommend users with shuffle-sensitive models to push their
shuffle quality higher until this threshold is hit.

What is Arrow and how does Ray Data use it?
===============================================

`Apache Arrow <https://arrow.apache.org/>`__ is a columnar memory format and a
single-node data processing and I/O library that Ray Data leverages extensively. You
can think of Ray Data as orchestrating distributed processing of Arrow data.

See our :ref:`key concepts <data_key_concepts>` for more information on how Ray Data
uses Arrow.

How much performance tuning does Ray Data require?
======================================================

Ray Data doesn't perform query optimization, so some manual performance
tuning may be necessary depending on your use case and data scale. Please see our
:ref:`performance tuning guide <data_performance_tips>` for more information.

What is strict mode?
====================

In Ray 2.5, Ray Data by default always requires data schemas, dropping support for
standalone Python objects. In addition to unification and simplicity benefits, this
aligns the Ray Data API closer to industry-standard distributed data APIs like Apache
Spark and also emerging standards for machine learning datasets like HuggingFace.

Migrating to strict mode
~~~~~~~~~~~~~~~~~~~~~~~~

You can disable strict mode temporarily by setting the environment variable
``RAY_DATA_STRICT_MODE=0`` on all cluster processes. Strict mode will not be
possible to disable in future releases.

Migrating existing code is straightforward. There are two common changes you may need
to make to your code to be compatible:

1. Pass the ``batch_format="pandas"`` argument to ``map_batches`` or ``iter_batches``,
   if your code assumes pandas is the default batch format.
2. Instead of returning a standalone objects or numpy arrays from ``map`` or ``map_batches``,
   return a dictionary that names the field. E.g., change function code from ``return object()`` to
   ``return {"my_obj": object()}``, and ``return [1, 2, 3]`` to ``return {"my_values": [1, 2, 3]}``.

List of strict mode changes
~~~~~~~~~~~~~~~~~~~~~~~~~~~

In more detail, support for standalone Python objects is dropped. This means that
instead of directly storing, e.g., Python ``Tuple[str, int]`` instance in Ray Data,
you must either give each field a name (i.e., ``{foo: str, bar: int}``), or
use a named object-type field (i.e., ``{foo: object}``). In addition, the ``default``
batch format is replaced with ``numpy`` by default. This means that most users
just need to be aware of ``Dict[str, Any]`` (non-batched data records) and
``Dict[str, np.ndarray]`` (batched data) types when working with Ray Data.

**Full list of changes**:

* All read apis return structured data, never standalone Python objects.
* Standalone Python objects are prohibited from being returned from map / map batches.
* Standalone Numpy arrays are prohibited from being returned from map / map batches.
* There is no more special interpretation of single-column schema containing just ``__value__`` as a column.
* The default batch format is ``numpy`` instead of ``default`` (pandas).
* ``schema()`` returns a unified Schema class instead of ``Union[pyarrow.lib.Schema, type]``.

**Datasource behavior changes**:

* ``range_tensor``: create ``data``  column instead of ``__value__``.
* ``from_numpy`` / ``from_numpy_refs`` : create ``data`` column instead of using ``__value__``.
* ``from_items``: create ``item`` column instead of using Python objects.
* ``range``: create ``id`` column instead of using Python objects.

How can I contribute to Ray Data?
=====================================

We're always happy to accept external contributions! If you have a question, a feature
request, or want to contibute to Ray Data or tell us about your use case, please
reach out to us on `Discourse <https://discuss.ray.io/>`__; if you have a you're
confident that you've found a bug, please open an issue on the
`Ray GitHub repo <https://github.com/ray-project/ray>`__. Please see our
:ref:`contributing guide <getting-involved>` for more information!
