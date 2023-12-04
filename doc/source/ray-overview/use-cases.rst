.. _ref-use-cases:

Ray Use Cases
=============

.. toctree::
    :hidden:

    ../ray-air/getting-started



.. raw:: html

    <link rel="stylesheet" type="text/css" href="../_static/css/use_cases.css">

This page indexes common Ray use cases for scaling ML.
It contains highlighted references to blogs, examples, and tutorials also located
elsewhere in the Ray documentation.

.. _ref-use-cases-llm:

LLMs and Gen AI
---------------

Large language models (LLMs) and generative AI are rapidly changing industries, and demand compute at an astonishing pace. Ray provides a distributed compute framework for scaling these models, allowing developers to train and deploy models faster and more efficiently. With specialized libraries for data streaming, training, fine-tuning, hyperparameter tuning, and serving, Ray simplifies the process of developing and deploying large-scale AI models.

.. figure:: /images/llm-stack.png

.. query-param-ref:: ray-overview/examples
    :parameters: ?tags=llm
    :ref-type: doc
    :classes: example-gallery-link

    Explore LLMs and Gen AI examples

.. _ref-use-cases-batch-infer:

Batch Inference
---------------

Batch inference is the process of generating model predictions on a large "batch" of input data.
Ray for batch inference works with any cloud provider and ML framework,
and is fast and cheap for modern deep learning applications.
It scales from single machines to large clusters with minimal code changes.
As a Python-first framework, you can easily express and interactively develop your inference workloads in Ray.
To learn more about running batch inference with Ray, see the :ref:`batch inference guide<batch_inference_home>`.

.. figure:: ../data/images/batch_inference.png

.. query-param-ref:: ray-overview/examples
    :parameters: ?tags=inference
    :ref-type: doc
    :classes: example-gallery-link

    Explore batch inference examples

.. _ref-use-cases-mmt:

Many Model Training
-------------------

Many model training is common in ML use cases such as time series forecasting, which require fitting of models on multiple data batches corresponding to locations, products, etc.
The focus is on training many models on subsets of a dataset. This is in contrast to training a single model on the entire dataset.

When any given model you want to train can fit on a single GPU, Ray can assign each training run to a separate Ray Task. In this way, all available workers are utilized to run independent remote training rather than one worker running jobs sequentially.

.. figure:: /images/training_small_models.png

  Data parallelism pattern for distributed training on large datasets.

How do I do many model training on Ray?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To train multiple independent models, use the Ray Tune (:ref:`Tutorial <mmt-tune>`) library. This is the recommended library for most cases.

You can use Tune with your current data preprocessing pipeline if your data source fits into the memory of a single machine (node).
If you need to scale your data, or you want to plan for future scaling, use the :ref:`Ray Data <data>` library.
Your data must be a :ref:`supported format <input-output>`, to use Ray Data.

Alternative solutions exist for less common cases:

#. If your data is not in a supported format, use Ray Core (:ref:`Tutorial <mmt-core>`) for custom applications. This is an advanced option and requires and understanding of :ref:`design patterns and anti-patterns <core-patterns>`.
#. If you have a large preprocessing pipeline, you can use the Ray Data library to train multiple models (:ref:`Tutorial <mmt-datasets>`).

.. query-param-ref:: ray-overview/examples
    :parameters: ?tags=training
    :ref-type: doc
    :classes: example-gallery-link

    Explore model training examples

.. _ref-use-cases-model-serving:

Model Serving
-------------

:ref:`Ray Serve <rayserve>` is well suited for model composition, enabling you to build a complex inference service consisting of multiple ML models and business logic all in Python code.

It supports complex `model deployment patterns <https://www.youtube.com/watch?v=mM4hJLelzSw>`_ requiring the orchestration of multiple Ray actors, where different actors provide inference for different models. Serve handles both batch and online inference and can scale to thousands of models in production.

.. figure:: /images/multi_model_serve.png

  Deployment patterns with Ray Serve. (Click image to enlarge.)

Learn more about model serving with the following resources.

- `[Talk] Productionizing ML at Scale with Ray Serve <https://www.youtube.com/watch?v=UtH-CMpmxvI>`_
- `[Blog] Simplify your MLOps with Ray & Ray Serve <https://www.anyscale.com/blog/simplify-your-mlops-with-ray-and-ray-serve>`_
- :doc:`[Guide] Getting Started with Ray Serve </serve/getting_started>`
- :doc:`[Guide] Model Composition in Serve </serve/model_composition>`
- :doc:`[Gallery] Serve Examples Gallery </serve/tutorials/index>`
- `[Gallery] More Serve Use Cases on the Blog <https://www.anyscale.com/blog?tag=ray_serve>`_

.. _ref-use-cases-hyperparameter-tuning:

Hyperparameter Tuning
---------------------

The :ref:`Ray Tune <tune-main>` library enables any parallel Ray workload to be run under a hyperparameter tuning algorithm.

Running multiple hyperparameter tuning experiments is a pattern apt for distributed computing because each experiment is independent of one another. Ray Tune handles the hard bit of distributing hyperparameter optimization and makes available key features such as checkpointing the best result, optimizing scheduling, and specifying search patterns.

.. figure:: /images/tuning_use_case.png

   Distributed tuning with distributed training per trial.

Learn more about the Tune library with the following talks and user guides.

- :doc:`[Guide] Getting Started with Ray Tune </tune/getting-started>`
- `[Blog] How to distribute hyperparameter tuning with Ray Tune <https://www.anyscale.com/blog/how-to-distribute-hyperparameter-tuning-using-ray-tune>`_
- `[Talk] Simple Distributed Hyperparameter Optimization <https://www.youtube.com/watch?v=KgYZtlbFYXE>`_
- `[Blog] Hyperparameter Search with 🤗 Transformers <https://www.anyscale.com/blog/hyperparameter-search-hugging-face-transformers-ray-tune>`_
- :doc:`[Gallery] Ray Tune Examples Gallery </tune/examples/index>`
- `More Tune use cases on the Blog <https://www.anyscale.com/blog?tag=ray-tune>`_

.. _ref-use-cases-distributed-training:

Distributed Training
--------------------

The :ref:`Ray Train <train-docs>` library integrates many distributed training frameworks under a simple Trainer API,
providing distributed orchestration and management capabilities out of the box.

In contrast to training many models, model parallelism partitions a large model across many machines for training. Ray Train has built-in abstractions for distributing shards of models and running training in parallel.

.. figure:: /images/model_parallelism.png

  Model parallelism pattern for distributed large model training.

Learn more about the Train library with the following talks and user guides.

- `[Talk] Ray Train, PyTorch, TorchX, and distributed deep learning <https://www.youtube.com/watch?v=e-A93QftCfc>`_
- `[Blog] Elastic Distributed Training with XGBoost on Ray <https://www.uber.com/blog/elastic-xgboost-ray/>`_
- :doc:`[Guide] Getting Started with Ray Train </train/train>`
- :doc:`[Example] Fine-tune a 🤗 Transformers model </train/examples/transformers/huggingface_text_classification>`
- :doc:`[Gallery] Ray Train Examples Gallery </train/examples>`
- `[Gallery] More Train Use Cases on the Blog <https://www.anyscale.com/blog?tag=ray_train>`_

.. _ref-use-cases-reinforcement-learning:

Reinforcement Learning
----------------------

RLlib is an open-source library for reinforcement learning (RL), offering support for production-level, highly distributed RL workloads while maintaining unified and simple APIs for a large variety of industry applications. RLlib is used by industry leaders in many different verticals, such as climate control, industrial control, manufacturing and logistics, finance, gaming, automobile, robotics, boat design, and many others.

.. figure:: /images/rllib_use_case.png

   Decentralized distributed proximal polixy optimiation (DD-PPO) architecture.

Learn more about reinforcement learning with the following resources.

- `[Course] Applied Reinforcement Learning with RLlib <https://applied-rl-course.netlify.app/>`_
- `[Blog] Intro to RLlib: Example Environments <https://medium.com/distributed-computing-with-ray/intro-to-rllib-example-environments-3a113f532c70>`_
- :doc:`[Guide] Getting Started with RLlib </rllib/rllib-training>`
- `[Talk] Deep reinforcement learning at Riot Games <https://www.anyscale.com/events/2022/03/29/deep-reinforcement-learning-at-riot-games>`_
- :doc:`[Gallery] RLlib Examples Gallery </rllib/rllib-examples>`
- `[Gallery] More RL Use Cases on the Blog <https://www.anyscale.com/blog?tag=rllib>`_

.. _ref-use-cases-ml-platform:

ML Platform
-----------

Ray and its AI libraries provide unified compute runtime for teams looking to simplify their ML platform.
Ray's libraries such as Ray Train, Ray Data, and Ray Serve can be used to compose end-to-end ML workflows, providing features and APIs for
data preprocessing as part of training, and transitioning from training to serving.

Read more about building ML platforms with Ray in :ref:`this section <ray-for-ml-infra>`.

..
  https://docs.google.com/drawings/d/1PFA0uJTq7SDKxzd7RHzjb5Sz3o1WvP13abEJbD0HXTE/edit

.. image:: /images/ray-air.svg

End-to-End ML Workflows
-----------------------

The following highlights examples utilizing Ray AI libraries to implement end-to-end ML workflows.

- :doc:`[Example] Text classification with Ray </train/examples/transformers/huggingface_text_classification>`
- :doc:`[Example] Object detection with Ray </train/examples/pytorch/torch_detection>`
- :doc:`[Example] Machine learning on tabular data </train/examples/xgboost/xgboost_example>`
- :doc:`[Example] AutoML for Time Series with Ray </ray-core/examples/automl_for_time_series>`

Large Scale Workload Orchestration
----------------------------------

The following highlights feature projects leveraging Ray Core's distributed APIs to simplify the orchestration of large scale workloads.

- `[Blog] Highly Available and Scalable Online Applications on Ray at Ant Group <https://www.anyscale.com/blog/building-highly-available-and-scalable-online-applications-on-ray-at-ant>`_
- `[Blog] Ray Forward 2022 Conference: Hyper-scale Ray Application Use Cases <https://www.anyscale.com/blog/ray-forward-2022>`_
- `[Blog] A new world record on the CloudSort benchmark using Ray <https://www.anyscale.com/blog/ray-breaks-the-usd1-tb-barrier-as-the-worlds-most-cost-efficient-sorting>`_
- :doc:`[Example] Speed up your web crawler by parallelizing it with Ray </ray-core/examples/web-crawler>`
