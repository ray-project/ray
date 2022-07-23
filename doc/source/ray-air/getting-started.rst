.. _air:

Ray AI Runtime (AIR)
====================

Ray AI Runtime (AIR) is an open-source toolkit for building end-to-end ML applications. By leveraging Ray, its distributed compute capabilities, and its library ecosystem, Ray AIR brings scalability and programmability to ML platforms.

.. tip::
    **Getting involved with Ray AIR.** Fill out `this short form <https://forms.gle/wCCdbaQDtgErYycT6>`__ to get involved. We'll be holding office hours, development sprints, and other activities as we get closer to the Ray AIR Beta/GA release. Join us!

.. image:: images/ai_runtime.jpg


Ray AIR focuses on two functional aspects:

* It provides scalability by leveraging Ray’s distributed compute layer for ML workloads.
* It is designed to interoperate with other systems for storage and metadata needs.

Ray AIR consists of five key components:

- Data processing (:ref:`Ray Data <datasets>`)
- Model Training (:ref:`Ray Train <train-docs>`)
- Reinforcement Learning (:ref:`Ray RLlib <rllib-index>`)
- Hyperparameter Tuning (:ref:`Ray Tune <tune-main>`)
- Model Serving (:ref:`Ray Serve <rayserve>`).

Users can use these libraries interchangeably to scale different parts of standard ML workflows.

To get started, install Ray AIR via ``pip install -U "ray[air]"``


Quick Start
-----------

Below, we demonstrate how you can use the Ray libraries in a seamless flow
between distributed frameworks (e.g., XGBoost, Pytorch, and Tensorflow): 

Preprocessing
~~~~~~~~~~~~~

Below, let's start by preprocessing your data with Ray AIR's ``Preprocessors``:

.. literalinclude:: examples/xgboost_starter.py
    :language: python
    :start-after: __air_generic_preprocess_start__
    :end-before: __air_generic_preprocess_end__

If using Tensorflow or Pytorch, format your data for use with your training framework:

.. tabbed:: XGBoost

    .. code-block:: python
        
        # No extra preprocessing is required for XGBoost.
        # The data is already in the correct format.

.. tabbed:: Pytorch

    .. literalinclude:: examples/pytorch_tabular_starter.py
        :language: python
        :start-after: __air_pytorch_preprocess_start__
        :end-before: __air_pytorch_preprocess_end__

.. tabbed:: Tensorflow

    .. literalinclude:: examples/tf_tabular_starter.py
        :language: python
        :start-after: __air_tf_preprocess_start__
        :end-before: __air_tf_preprocess_end__

Training
~~~~~~~~

Train a model with a ``Trainer`` with common ML frameworks:

.. tabbed:: XGBoost

    .. literalinclude:: examples/xgboost_starter.py
        :language: python
        :start-after: __air_xgb_train_start__
        :end-before: __air_xgb_train_end__

.. tabbed:: Pytorch

    .. literalinclude:: examples/pytorch_tabular_starter.py
        :language: python
        :start-after: __air_pytorch_train_start__
        :end-before: __air_pytorch_train_end__

.. tabbed:: Tensorflow

    .. literalinclude:: examples/tf_tabular_starter.py
        :language: python
        :start-after: __air_tf_train_start__
        :end-before: __air_tf_train_end__

Hyperparameter Tuning
~~~~~~~~~~~~~~~~~~~~~

You can specify a hyperparameter space to search over for each trainer:

.. tabbed:: XGBoost

    .. literalinclude:: examples/xgboost_starter.py
        :language: python
        :start-after: __air_xgb_tuner_start__
        :end-before: __air_xgb_tuner_end__

.. tabbed:: Pytorch

    .. literalinclude:: examples/pytorch_tabular_starter.py
        :language: python
        :start-after: __air_pytorch_tuner_start__
        :end-before: __air_pytorch_tuner_end__

.. tabbed:: Tensorflow

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

Use the trained model for scalable batch prediction with a ``BatchPredictor``.

.. tabbed:: XGBoost

    .. literalinclude:: examples/xgboost_starter.py
        :language: python
        :start-after: __air_xgb_batchpred_start__
        :end-before: __air_xgb_batchpred_end__

.. tabbed:: Pytorch

    .. literalinclude:: examples/pytorch_tabular_starter.py
        :language: python
        :start-after: __air_pytorch_batchpred_start__
        :end-before: __air_pytorch_batchpred_end__

.. tabbed:: Tensorflow

    .. literalinclude:: examples/tf_tabular_starter.py
        :language: python
        :start-after: __air_tf_batchpred_start__
        :end-before: __air_tf_batchpred_end__


See the :ref:`Key Concepts <air-key-concepts>` for more that Ray AIR has to offer.


Why Ray AIR?
------------

Today, there are a myriad of machine learning frameworks, platforms, and tools. Why would you choose Ray AIR and what makes it different? Ray AIR provides three 
unique functional values derived from Ray. Let's examine each.

**1. Seamless development to production**: Ray AIR reduces development friction going from development to production. Unlike in other frameworks, scaling Ray applications from a laptop to large clusters doesn't require a separate way of running -- the same code scales up seamlessly.
This means data scientists and ML practitioners spend less time fighting YAMLs and refactoring code. Smaller teams and companies that don’t have the resources to invest heavily on MLOps can now deploy ML models at a much faster rate with Ray AIR.


**2. Multi-cloud and Framework-interoperable**: Ray AIR is multi-cloud and framework-interoperable. The Ray compute layer and libraries freely operate with common public cloud platforms and frameworks in the ecosystem, reducing lock-in to any particular choices of ML tech. Framework interoperability is unique to Ray--- it's easy to run Torch distributed or elastic Horovod within Ray, but not vice versa.

**3. Future-proof via flexibility and scalability**: Ray's scalability and flexibility make Ray AIR future-proof. Advanced serving pipelines, elastic training, online learning, reinforcement learning applications are being built and scaled today on Ray. Common model deployment patterns are being incorporated into libraries like Ray Serve.

AIR Ecosystem
-------------

AIR is currently in *beta*, but some components are more stable. The following diagram provides an overview of the AIR components, ecosystem integrations, and their readiness.

..
  https://docs.google.com/drawings/d/1pZkRrkAbRD8jM-xlGlAaVo3T66oBQ_HpsCzomMT7OIc/edit

.. image:: images/air-ecosystem.svg

Next Steps
----------

- :ref:`air-key-concepts`
- `Examples <https://github.com/ray-project/ray/tree/master/python/ray/air/examples>`__
- :ref:`Deployment Guide <air-deployment>`
- :ref:`API reference <air-api-ref>`
