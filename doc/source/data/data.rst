.. _data:

==================================
Ray Data: Scalable Datasets for ML
==================================

.. toctree::
    :hidden:

    quickstart
    Concepts <key-concepts>
    user-guide
    examples
    api/api
    comparisons
    data-internals

Ray Data is a scalable data processing library for ML and AI workloads built on Ray.
Ray Data provides flexible and performant APIs for expressing AI workloads such as :ref:`batch inference <batch_inference_home>`, data preprocessing, and ingest for ML training. Unlike other distributed data systems, Ray Data features a :ref:`streaming execution <streaming-execution>` to efficiently process large datasets and maintain high utilization across both CPU and GPU workloads.


Why choose Ray Data?
--------------------

Modern AI workloads revolve around the usage of deep learning models, which are computationally intensive and often require specialized hardware such as GPUs.
Unlike CPUs, GPUs often come with less memory, have different semantics for scheduling, and are much more expensive to run.
Systems built to support traditional data processing pipelines often don't utilize such resources well.

Ray Data supports AI workloads as a first-class citizen and offers several key advantages:

- **Faster and cheaper for deep learning**: Ray Data streams data between CPU preprocessing and GPU inference/training tasks, maximizing resource utilization and reducing costs by keeping GPUs active.

- **Framework friendly**: Ray Data provides performant, first-class integration with common AI frameworks (vLLM, PyTorch, HuggingFace, TensorFlow) and common cloud providers (AWS, GCP, Azure)

- **Support for multi-modal data**: Ray Data leverages Apache Arrow and Pandas and provides support for many data formats used in ML workloads such as Parquet, Lance, images, JSON, CSV, audio, video, and more.

- **Scalable by default**: Built on Ray for automatic scaling across heterogeneous clusters with different CPU and GPU machines. Code runs unchanged from one machine to hundreds of nodes processing hundreds of TB of data.

..
  https://docs.google.com/drawings/d/16AwJeBNR46_TsrkOmMbGaBK7u-OPsf_V8fHjU-d2PPQ/edit

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
- `DoorDash elevates model training with Ray Data <https://raysummit.anyscale.com/agenda/sessions/144>`_
- `Instacart builds distributed machine learning model training on Ray Data <https://tech.instacart.com/distributed-machine-learning-at-instacart-4b11d7569423>`_
- `Predibase speeds up image augmentation for model training using Ray Data <https://predibase.com/blog/ludwig-v0-7-fine-tuning-pretrained-image-and-text-models-50x-faster-and>`_

**Batch inference using Ray Data**

- `ByteDance scales offline inference with multi-modal LLMs to 200 TB on Ray Data <https://www.anyscale.com/blog/how-bytedance-scales-offline-inference-with-multi-modal-llms-to-200TB-data>`_
- `Spotify's new ML platform built on Ray Data for batch inference <https://engineering.atspotify.com/2023/02/unleashing-ml-innovation-at-spotify-with-ray/>`_
- `Sewer AI speeds up object detection on videos 3x using Ray Data <https://www.anyscale.com/blog/inspecting-sewer-line-safety-using-thousands-of-hours-of-video>`_
