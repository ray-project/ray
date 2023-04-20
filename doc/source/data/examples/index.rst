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

.. grid:: 3
    :gutter: 2
    :class-container: container pb-4

    .. grid-item-card::
        :img-top: /images/taxi.png
        :class-img-top: pt-5 w-75 d-block mx-auto

        +++
        .. button-ref:: nyc_taxi_basic_processing
            :ref-type: ref
            :color: primary
            :outline:
            :expand:
            :click-parent:

            Processing the NYC taxi dataset

    .. grid-item-card::
        :img-top: /images/taxi.png
        :class-img-top: pt-5 w-75 d-block mx-auto

        +++
        .. button-ref:: batch_training
            :ref-type: ref
            :color: primary
            :outline:
            :expand:
            :click-parent:

            Batch Training with Ray Data

    .. grid-item-card::
        :img-top: /images/ocr.jpg
        :class-img-top: pt-5 w-75 d-block mx-auto

        +++
        .. button-ref:: ocr_example
            :ref-type: ref
            :color: primary
            :outline:
            :expand:
            :click-parent:

            Scaling OCR with Ray Data



Other Examples
--------------

.. panels::
    :container: container pb-4
    :column: col-md-4 px-2 py-2
    :img-top-cls: pt-5 w-75 d-block mx-auto

    ---
    :img-top: ../images/datastream-arch.svg

    +++
    .. link-button:: random-access
        :type: ref
        :text: Random Data Access (Experimental)
        :classes: btn-link btn-block stretched-link
