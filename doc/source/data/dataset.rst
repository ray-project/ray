.. include:: /_includes/data/announcement.rst

.. _datasets:

============================================
Ray Datasets: Distributed Data Preprocessing
============================================

.. _datasets-intro:

Ray Datasets are the standard way to load and exchange data in Ray libraries and applications.
They provide basic distributed data transformations such as maps
(:meth:`map_batches <ray.data.Dataset.map_batches>`),
global and grouped aggregations (:class:`GroupedDataset <ray.data.grouped_dataset.GroupedDataset>`), and
shuffling operations (:meth:`random_shuffle <ray.data.Dataset.random_shuffle>`,
:meth:`sort <ray.data.Dataset.sort>`,
:meth:`repartition <ray.data.Dataset.repartition>`),
and are compatible with a variety of file formats, data sources, and distributed frameworks.

Here's an overview of the integrations with other processing frameworks, file formats, and supported operations,
as well as a glimpse at the Ray Datasets API.

Check the :ref:`Input/Output reference <input-output>` to see if your favorite format
is already supported.

.. image:: images/dataset.svg

..
  https://docs.google.com/drawings/d/16AwJeBNR46_TsrkOmMbGaBK7u-OPsf_V8fHjU-d2PPQ/edit


----------------------------------------------
Data Loading and Preprocessing for ML Training
----------------------------------------------

Use Ray Datasets to load and preprocess data for distributed :ref:`ML training pipelines <train-docs>`.
Compared to other loading solutions, Datasets are more flexible (e.g., can express higher-quality per-epoch global shuffles) and provides `higher overall performance <https://www.anyscale.com/blog/why-third-generation-ml-platforms-are-more-performant>`__.

Use Datasets as a last-mile bridge from storage or ETL pipeline outputs to distributed 
applications and libraries in Ray. Don't use it as a replacement for more general data 
processing systems.

.. image:: images/dataset-loading-1.png
   :width: 650px
   :align: center

..
  https://docs.google.com/presentation/d/1l03C1-4jsujvEFZUM4JVNy8Ju8jnY5Lc_3q7MBWi2PQ/edit

To learn more about the features Datasets supports, read the 
:ref:`Datasets User Guide <data_user_guide>`.

-----------------------------
Datasets for Parallel Compute
-----------------------------

Datasets also simplify general purpose parallel GPU and CPU compute in Ray; for
instance, for :ref:`GPU batch inference <transforming_datasets>`.
They provide a higher-level API for Ray tasks and actors for such embarrassingly parallel compute,
internally handling operations like batching, pipelining, and memory management.

.. image:: images/dataset-compute-1.png
   :width: 500px
   :align: center

As part of the Ray ecosystem, Ray Datasets can leverage the full functionality of Ray's distributed scheduler,
e.g., using actors for optimizing setup time and GPU scheduling.

----------------------
Where to Go from Here?
----------------------

As new user of Ray Datasets, you may want to start with our :ref:`Getting Started guide<datasets_getting_started>`.
If you've run your first examples already, you might want to dive into Ray Datasets'
:ref:`key concepts <data_key_concepts>` or our :ref:`User Guide <data_user_guide>` instead.
Advanced users can refer directly to the Ray Datasets :ref:`API reference <data-api>` for their projects.

.. panels::
    :container: text-center
    :column: col-lg-6 px-2 py-2
    :card:

    **Getting Started**
    ^^^

    Start with our quick start tutorials for working with Datasets.
    These concrete examples will give you an idea of how to use Ray Datasets.

    +++
    .. link-button:: datasets_getting_started
        :type: ref
        :text: Get Started with Ray Datasets
        :classes: btn-outline-info btn-block
    ---

    **Key Concepts**
    ^^^

    Understand the key concepts behind Ray Datasets.
    Learn what :ref:`Datasets <dataset_concept>` are and how they are executed in Ray
    Datasets.

    +++
    .. link-button:: data_key_concepts
        :type: ref
        :text: Learn Key Concepts
        :classes: btn-outline-info btn-block
    ---

    **User Guides**
    ^^^

    Learn how to :ref:`create datasets <creating_datasets>`, :ref:`save
    datasets <saving_datasets>`, :ref:`transform datasets <transforming_datasets>`,
    :ref:`access and exchange datasets <consuming_datasets>`, :ref:`pipeline
    transformations <pipelining_datasets>`, or 
    :ref:`work with tensor data <datasets_tensor_support>`.

    +++
    .. link-button:: data_user_guide
        :type: ref
        :text: Start Using Ray Datasets
        :classes: btn-outline-info btn-block
    ---

    **Examples**
    ^^^

    Find both simple and scaling-out examples of using Ray Datasets for data
    processing and ML ingest.

    +++
    .. link-button:: datasets-recipes
        :type: ref
        :text: Ray Datasets Examples
        :classes: btn-outline-info btn-block
    ---

    **Ray Datasets FAQ**
    ^^^

    Find answers to commonly asked questions in our detailed FAQ.

    +++
    .. link-button:: datasets_faq
        :type: ref
        :text: Ray Datasets FAQ
        :classes: btn-outline-info btn-block
    ---

    **API**
    ^^^

    Get more in-depth information about the Ray Datasets API.

    +++
    .. link-button:: data-api
        :type: ref
        :text: Read the API Reference
        :classes: btn-outline-info btn-block
    ---

    **Other Data Processing Solutions**
    ^^^

    For running ETL pipelines, check out :ref:`Spark-on-Ray <spark-on-ray>`. For scaling
    up your data science workloads, check out :ref:`Dask-on-Ray <dask-on-ray>`,
    :ref:`Modin <modin-on-ray>`, and :ref:`Mars-on-Ray <mars-on-ray>`.

    +++
    .. link-button:: integrations
        :type: ref
        :text: Check Out Other Data Processing Options
        :classes: btn-outline-info btn-block

------------------------
Datasource Compatibility
------------------------

Ray Datasets supports reading and writing many file formats.
To view supported formats, read the :ref:`Input/Output reference <input-output>`.

If your use case isn't supported, reach out on `Discourse <https://discuss.ray.io/>`__ or open a feature
request on the `Ray GitHub repo <https://github.com/ray-project/ray>`__, and check out
our :ref:`guide for implementing a custom Datasets datasource <datasets_custom_datasource>`
if you're interested in rolling your own integration!

.. _data-talks:

----------
Learn More
----------

- [slides] `Talk given at PyData 2021 <https://docs.google.com/presentation/d/1zANPlmrxQkjPU62I-p92oFO3rJrmjVhs73hL4YbM4C4>`_
- [blog] `Data Ingest in a Third Generation ML Architecture <https://www.anyscale.com/blog/deep-dive-data-ingest-in-a-third-generation-ml-architecture>`_
- [blog] `Building an end-to-end ML pipeline using Mars and XGBoost on Ray <https://www.anyscale.com/blog/building-an-end-to-end-ml-pipeline-using-mars-and-xgboost-on-ray>`_
- [blog] `Ray Datasets for large-scale machine learning ingest and scoring <https://www.anyscale.com/blog/ray-datasets-for-machine-learning-training-and-scoring>`_

----------
Contribute
----------

Contributions to Ray Datasets are :ref:`welcome <getting-involved>`!
There are many potential improvements, including:

- Supporting more data sources and transforms.
- Integration with more ecosystem libraries.
- Adding features such as `join()`.
- Performance optimizations.

.. include:: /_includes/data/announcement_bottom.rst
