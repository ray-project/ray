.. _data:

==========================================
Ray Data: Data Processing for AI Workloads
==========================================

.. toctree::
    :hidden:

    quickstart
    key-concepts
    user-guide
    examples
    api/api
    comparisons
    data-internals

Ray Data is a data processing library for AI workloads built on Ray.
Ray Data aims to bridge the gap between the needs of AI workloads and the capabilities of existing data processing systems
by providing:

- key primitives for efficient GPU batch inference, distributed training ingestion, and data preprocessing
- support for many data formats used in Data and AI workloads (Iceberg, Parquet, Lance, images, audio, video, etc.)
- integration with common AI frameworks (vLLM, Pytorch, Tensorflow, etc)
- support for distributed data processing (map, filter, join, groupby, aggregate, etc.)


Why choose Ray Data?
--------------------

Traditional data processing workloads are built on the following assumptions:

- Data formats are primarily *tabular*
- Computation is primarily *CPU or memory-bound*
- Ecosystems are primarily *Java or Scala* based.


However, modern AI workloads revolve around the usage of deep learning models, which are computationally intensive and often require specialized hardware such as GPUs.
These workloads result in the following characteristics:

- Data formats are mixed between *tabular* and *tensor*
- Computation is primarily *GPU-bound*
- Ecosystems are primarily *Python* based.


Ray Data is designed to address these characteristics by providing several key advantages:

- **GPU-friendly**: Ray Data is built to target GPU efficiency. Ray Data's architecture can stream data between CPU preprocessing and GPU inference/training tasks, maximizing resource utilization and reducing costs by keeping GPUs active.

- **Support for tabular and tensor data**: Ray Data has native support for various tensor types and can leverage many data formats used in Data and AI workloads such as Parquet, Lance, images, JSON, CSV, and more.

- **Centered on the AI Ecosystem**: Ray Data provides performant integrations with common AI frameworks (vLLM, PyTorch, HuggingFace, TensorFlow) and common cloud providers (AWS, GCP, Azure)



Install Ray Data
----------------

To install Ray Data, run:

.. code-block:: console

    $ pip install -U 'ray[data]'

To learn more about installing Ray and its libraries, see
:ref:`Installing Ray <installation>`.

Learn more
----------

.. grid:: 1 2 2 2
    :gutter: 1
    :class-container: container pb-5

    .. grid-item-card::

        **Quickstart**
        ^^^

        Get started with Ray Data with a simple example.

        +++
        .. button-ref:: data_quickstart
            :color: primary
            :outline:
            :expand:

            Quickstart

    .. grid-item-card::

        **Key Concepts**
        ^^^

        Learn the key concepts behind Ray Data. Learn what
        Datasets are and how they're used.

        +++
        .. button-ref:: data_key_concepts
            :color: primary
            :outline:
            :expand:

            Key Concepts

    .. grid-item-card::

        **User Guides**
        ^^^

        Learn how to use Ray Data, from basic usage to end-to-end guides.

        +++
        .. button-ref:: data_user_guide
            :color: primary
            :outline:
            :expand:

            Learn how to use Ray Data

    .. grid-item-card::

        **Examples**
        ^^^

        Find both simple and scaling-out examples of using Ray Data.

        +++
        .. button-ref:: examples
            :color: primary
            :outline:
            :expand:

            Ray Data Examples

    .. grid-item-card::

        **API**
        ^^^

        Get more in-depth information about the Ray Data API.

        +++
        .. button-ref:: data-api
            :color: primary
            :outline:
            :expand:

            Read the API Reference


Case studies for Ray Data
-------------------------

**Training ingest using Ray Data**

- `Pinterest uses Ray Data to do last mile data processing for model training <https://medium.com/pinterest-engineering/last-mile-data-processing-with-ray-629affbf34ff>`_
- `DoorDash elevates model training with Ray Data <https://www.youtube.com/watch?v=pzemMnpctVY>`_
- `Instacart builds distributed machine learning model training on Ray Data <https://tech.instacart.com/distributed-machine-learning-at-instacart-4b11d7569423>`_
- `Predibase speeds up image augmentation for model training using Ray Data <https://predibase.com/blog/ludwig-v0-7-fine-tuning-pretrained-image-and-text-models-50x-faster-and>`_

**Batch inference using Ray Data**

- `ByteDance scales offline inference with multi-modal LLMs to 200 TB on Ray Data <https://www.anyscale.com/blog/how-bytedance-scales-offline-inference-with-multi-modal-llms-to-200TB-data>`_
- `Spotify's new ML platform built on Ray Data for batch inference <https://engineering.atspotify.com/2023/02/unleashing-ml-innovation-at-spotify-with-ray/>`_
- `Sewer AI speeds up object detection on videos 3x using Ray Data <https://www.anyscale.com/blog/inspecting-sewer-line-safety-using-thousands-of-hours-of-video>`_
