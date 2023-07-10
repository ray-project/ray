.. _ray-for-ml-infra:

Ray for ML Infrastructure
=========================

.. tip::

    We'd love to hear from you if you are using Ray to build a ML platform! Fill out `this short form <https://forms.gle/wCCdbaQDtgErYycT6>`__ to get involved.

Ray and its AI Runtime libraries provide unified compute runtime for teams looking to simplify their ML platform.
Ray's libraries such as Ray Train, Ray Data, and Ray Serve can be used to compose end-to-end ML workflows, providing features and APIs for
data preprocessing as part of training, and transitioning from training to serving.

..
  https://docs.google.com/drawings/d/1atB1dLjZIi8ibJ2-CoHdd3Zzyl_hDRWyK2CJAVBBLdU/edit

.. image:: /images/ray-air.svg

Why Ray for ML Infrastructure?
------------------------------

Ray's AI Runtime libraries simplify the ecosystem of machine learning frameworks, platforms, and tools, by providing a seamless, unified, and open experience for scalable ML:


.. image:: images/why-air-2.svg

..
  https://docs.google.com/drawings/d/1oi_JwNHXVgtR_9iTdbecquesUd4hOk0dWgHaTaFj6gk/edit

**1. Seamless Dev to Prod**: Ray's AI Runtime libraries reduces friction going from development to production. With Ray and its libraries, the same Python code scales seamlessly from a laptop to a large cluster.

**2. Unified ML API and Runtime**: Ray's APIs enables swapping between popular frameworks, such as XGBoost, PyTorch, and Hugging Face, with minimal code changes. Everything from training to serving runs on a single runtime (Ray + KubeRay).

**3. Open and Extensible**: Ray is fully open-source and can run on any cluster, cloud, or Kubernetes. Build custom components and integrations on top of scalable developer APIs.

Example ML Platforms built on Ray
---------------------------------

`Merlin <https://shopify.engineering/merlin-shopify-machine-learning-platform>`_ is Shopify's ML platform built on Ray. It enables fast-iteration and `scaling of distributed applications <https://www.youtube.com/watch?v=kbvzvdKH7bc>`_ such as product categorization and recommendations.

.. figure:: /images/shopify-workload.png

  Shopify's Merlin architecture built on Ray.

Spotify `uses Ray for advanced applications <https://www.anyscale.com/ray-summit-2022/agenda/sessions/180>`_ that include personalizing content recommendations for home podcasts, and personalizing Spotify Radio track sequencing.

.. figure:: /images/spotify.png

  How Ray ecosystem empowers ML scientists and engineers at Spotify..


The following highlights feature companies leveraging Ray's unified API to build simpler, more flexible ML platforms.

- `[Blog] The Magic of Merlin - Shopify's New ML Platform <https://shopify.engineering/merlin-shopify-machine-learning-platform>`_
- `[Slides] Large Scale Deep Learning Training and Tuning with Ray <https://drive.google.com/file/d/1BS5lfXfuG5bnI8UM6FdUrR7CiSuWqdLn/view>`_
- `[Blog] Griffin: How Instacart’s ML Platform Tripled in a year <https://www.instacart.com/company/how-its-made/griffin-how-instacarts-ml-platform-tripled-ml-applications-in-a-year/>`_
- `[Talk] Predibase - A low-code deep learning platform built for scale <https://www.youtube.com/watch?v=B5v9B5VSI7Q>`_
- `[Blog] Building a ML Platform with Kubeflow and Ray on GKE <https://cloud.google.com/blog/products/ai-machine-learning/build-a-ml-platform-with-kubeflow-and-ray-on-gke>`_
- `[Talk] Ray Summit Panel - ML Platform on Ray <https://www.youtube.com/watch?v=_L0lsShbKaY>`_


.. Deployments on Ray.
.. include:: /ray-air/deployment.rst


What's Next
-----------

Below, we walk through how AIR's unified ML API enables scaling of end-to-end ML workflows, focusing on
a few of the popular frameworks AIR integrates with (XGBoost, Pytorch, and Tensorflow). The ML workflow we're going to build is summarized by the following diagram:

..
  https://docs.google.com/drawings/d/1z0r_Yc7-0NAPVsP2jWUkLV2jHVHdcJHdt9uN1GDANSY/edit

.. figure:: images/why-air.svg

  AIR provides a unified API for the ML ecosystem.
  This diagram shows how AIR enables an ecosystem of libraries to be run at scale in just a few lines of code.

Get started by installing Ray AIR:

.. code:: bash

    pip install -U "ray[air]"

    # The below Ray AIR tutorial was written with the following libraries.
    # Consider running the following to ensure that the code below runs properly:
    pip install -U pandas>=1.3.5
    pip install -U torch>=1.12
    pip install -U numpy>=1.19.5
    pip install -U tensorflow>=2.6.2
    pip install -U pyarrow>=6.0.1

Preprocessing
~~~~~~~~~~~~~

First, let's start by loading a dataset from storage:

.. literalinclude:: examples/xgboost_starter.py
    :language: python
    :start-after: __air_generic_preprocess_start__
    :end-before: __air_generic_preprocess_end__

Then, we define a ``Preprocessor`` pipeline for our task:

.. tabs::

    .. group-tab:: XGBoost

        .. literalinclude:: examples/xgboost_starter.py
            :language: python
            :start-after: __air_xgb_preprocess_start__
            :end-before: __air_xgb_preprocess_end__

    .. group-tab:: Pytorch

        .. literalinclude:: examples/pytorch_tabular_starter.py
            :language: python
            :start-after: __air_pytorch_preprocess_start__
            :end-before: __air_pytorch_preprocess_end__

    .. group-tab:: Tensorflow

        .. literalinclude:: examples/tf_tabular_starter.py
            :language: python
            :start-after: __air_tf_preprocess_start__
            :end-before: __air_tf_preprocess_end__

.. _air-getting-started-training:

Training
~~~~~~~~

Train a model with a ``Trainer`` with common ML frameworks:

.. tabs::

    .. group-tab:: XGBoost

        .. literalinclude:: examples/xgboost_starter.py
            :language: python
            :start-after: __air_xgb_train_start__
            :end-before: __air_xgb_train_end__

    .. group-tab:: Pytorch

        .. literalinclude:: examples/pytorch_tabular_starter.py
            :language: python
            :start-after: __air_pytorch_train_start__
            :end-before: __air_pytorch_train_end__

    .. group-tab:: Tensorflow

        .. literalinclude:: examples/tf_tabular_starter.py
            :language: python
            :start-after: __air_tf_train_start__
            :end-before: __air_tf_train_end__

.. _air-getting-started-tuning:

Hyperparameter Tuning
~~~~~~~~~~~~~~~~~~~~~

You can specify a hyperparameter space to search over for each trainer:

.. tabs::

    .. group-tab:: XGBoost

        .. literalinclude:: examples/xgboost_starter.py
            :language: python
            :start-after: __air_xgb_tuner_start__
            :end-before: __air_xgb_tuner_end__

    .. group-tab:: Pytorch

        .. literalinclude:: examples/pytorch_tabular_starter.py
            :language: python
            :start-after: __air_pytorch_tuner_start__
            :end-before: __air_pytorch_tuner_end__

    .. group-tab:: Tensorflow

        .. literalinclude:: examples/tf_tabular_starter.py
            :language: python
            :start-after: __air_tf_tuner_start__
            :end-before: __air_tf_tuner_end__

Then use the ``Tuner`` to run the search:

.. literalinclude:: examples/pytorch_tabular_starter.py
    :language: python
    :start-after: __air_tune_generic_start__
    :end-before: __air_tune_generic_end__

Batch Inference
~~~~~~~~~~~~~~~

After running the steps in :ref:`Training <air-getting-started-training>` or
:ref:`Tuning <air-getting-started-tuning>`, use the trained model for scalable batch
prediction with :meth:`Dataset.map_batches() <ray.data.Dataset.map_batches>`.

To learn more, see :ref:`End-to-end: Offline Batch Inference <batch_inference_home>`.


Project Status
--------------

AIR is currently in **beta**. If you have questions for the team or are interested in getting involved in the development process, fill out `this short form <https://forms.gle/wCCdbaQDtgErYycT6>`__.

For an overview of the AIR libraries, ecosystem integrations, and their readiness, check out the latest :ref:`AIR ecosystem map <air-ecosystem-map>`.

Next Steps
----------

- :ref:`air-key-concepts`
- :ref:`air-examples-ref`
- :ref:`API reference <air-api-ref>`
- :ref:`Technical whitepaper <whitepaper>`
- To check how your application is doing, you can use the :ref:`Ray dashboard<observability-getting-started>`.
