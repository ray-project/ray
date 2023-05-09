.. _data-examples-ref:

========
Examples
========

.. tip:: Check out the Datastreams :ref:`User Guide <data_user_guide>` to learn more about
  Datastream features in-depth.

.. _data-recipes:

Simple Data Processing Examples
-------------------------------

Ray Data is a data processing engine that supports multiple data
modalities and types. Here you will find a few end-to-end examples of some basic data
processing with Ray Data on tabular data, text (coming soon!), and imagery (coming
soon!).

.. grid:: 1 2 3 3
    :gutter: 2
    :class-container: container pb-4

    .. grid-item-card::
        :img-top: /images/taxi.png
        :class-img-top: pt-5 w-75 d-block mx-auto

        .. button-ref:: nyc_taxi_basic_processing

            Processing the NYC taxi dataset

    .. grid-item-card::
        :img-top: /images/taxi.png
        :class-img-top: pt-5 w-75 d-block mx-auto

        .. button-ref:: batch_training

            Batch Training with Ray Data

    .. grid-item-card::
        :img-top: /images/ocr.jpg
        :class-img-top: pt-5 w-75 d-block mx-auto

        .. button-ref:: ocr_example

            Scaling OCR with Ray Data



Other Examples
--------------


.. grid:: 1 2 3 3
    :gutter: 2
    :class-container: container pb-4

    .. grid-item-card::
        :img-top: ../images/datastream-arch.svg
        :class-img-top: pt-5 w-75 d-block mx-auto

        .. button-ref:: random-access

            Random Data Access (Experimental)
