.. _datasets-examples-ref:

========
Examples
========

.. tip:: Check out the Datasets :ref:`User Guide <data_user_guide>` to learn more about
  Datasets' features in-depth.

.. _datasets-recipes:

Simple Data Processing Examples
-------------------------------

Ray Datasets is a data processing engine that supports multiple data
modalities and types. Here you will find a few end-to-end examples of some basic data
processing with Ray Datasets on tabular data, text (coming soon!), and imagery (coming
soon!).

.. panels::
    :container: container pb-4
    :column: col-md-4 px-2 py-2
    :img-top-cls: pt-5 w-75 d-block mx-auto

    ---
    :img-top: /images/taxi.png

    +++
    .. link-button:: nyc_taxi_basic_processing
        :type: ref
        :text: Processing NYC taxi data using Ray Datasets
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/taxi.png

    +++
    .. link-button:: batch_training
        :type: ref
        :text: Batch Training on NYC taxi data using Ray Datasets
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/ocr.jpg

    +++
    .. link-button:: ocr_example
        :type: ref
        :text: Optical character recognition using Ray Datasets
        :classes: btn-link btn-block stretched-link


Scaling Out Datasets Workloads
------------------------------

These examples demonstrate using Ray Datasets on large-scale data over a multi-node Ray
cluster.

.. panels::
    :container: container pb-4
    :column: col-md-4 px-2 py-2
    :img-top-cls: pt-5 w-75 d-block mx-auto

    ---
    :img-top: /images/dataset-repeat-2.svg

    +++
    .. link-button:: big_data_ingestion
        :type: ref
        :text: Large-scale ML Ingest
        :classes: btn-link btn-block stretched-link
