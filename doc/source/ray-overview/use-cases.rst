.. _ref-use-cases:

Ray Use Cases
=============

.. toctree::
    :hidden:

    ../ray-air/getting-started


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

    .. raw:: html

        <svg width='24' height='24' viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg' class='star-icon'>
            <g>
                <path class='star-icon-path' d='M15.199 9.945C14.7653 9.53412 14.4863 8.98641 14.409 8.394L14.006 5.311L11.276 6.797C10.7511 7.08302 10.1436 7.17943 9.55597 7.07L6.49997 6.5L7.06997 9.556C7.1794 10.1437 7.08299 10.7511 6.79697 11.276L5.31097 14.006L8.39397 14.409C8.98603 14.4865 9.53335 14.7655 9.94397 15.199L12.082 17.456L13.418 14.649C13.6744 14.1096 14.1087 13.6749 14.648 13.418L17.456 12.082L15.199 9.945ZM15.224 15.508L13.011 20.158C12.9691 20.2459 12.9065 20.3223 12.8285 20.3806C12.7505 20.4389 12.6594 20.4774 12.5633 20.4926C12.4671 20.5079 12.3686 20.4995 12.2764 20.4682C12.1842 20.4369 12.101 20.3836 12.034 20.313L8.49197 16.574C8.39735 16.4742 8.27131 16.41 8.13497 16.392L3.02797 15.724C2.93149 15.7113 2.83954 15.6753 2.76006 15.6191C2.68058 15.563 2.61596 15.4883 2.57177 15.4016C2.52758 15.3149 2.50514 15.2187 2.5064 15.1214C2.50765 15.0241 2.53256 14.9285 2.57897 14.843L5.04097 10.319C5.10642 10.198 5.12831 10.0582 5.10297 9.923L4.15997 4.86C4.14207 4.76417 4.14778 4.66541 4.17662 4.57229C4.20546 4.47916 4.25656 4.39446 4.3255 4.32553C4.39444 4.25659 4.47913 4.20549 4.57226 4.17665C4.66539 4.14781 4.76414 4.14209 4.85997 4.16L9.92297 5.103C10.0582 5.12834 10.198 5.10645 10.319 5.041L14.843 2.579C14.9286 2.53257 15.0242 2.50769 15.1216 2.50648C15.219 2.50528 15.3152 2.52781 15.4019 2.57211C15.4887 2.61641 15.5633 2.68116 15.6194 2.76076C15.6755 2.84036 15.7114 2.93242 15.724 3.029L16.392 8.135C16.4099 8.27134 16.4742 8.39737 16.574 8.492L20.313 12.034C20.3836 12.101 20.4369 12.1842 20.4682 12.2765C20.4995 12.3687 20.5079 12.4671 20.4926 12.5633C20.4774 12.6595 20.4389 12.7505 20.3806 12.8285C20.3223 12.9065 20.2459 12.9691 20.158 13.011L15.508 15.224C15.3835 15.2832 15.2832 15.3835 15.224 15.508ZM16.021 17.435L17.435 16.021L21.678 20.263L20.263 21.678L16.021 17.435Z'> </path>
            </g>
        </svg>Explore LLMs and Gen AI examples

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

    .. raw:: html

        <svg width='24' height='24' viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg' class='star-icon'>
            <g>
                <path class='star-icon-path' d='M15.199 9.945C14.7653 9.53412 14.4863 8.98641 14.409 8.394L14.006 5.311L11.276 6.797C10.7511 7.08302 10.1436 7.17943 9.55597 7.07L6.49997 6.5L7.06997 9.556C7.1794 10.1437 7.08299 10.7511 6.79697 11.276L5.31097 14.006L8.39397 14.409C8.98603 14.4865 9.53335 14.7655 9.94397 15.199L12.082 17.456L13.418 14.649C13.6744 14.1096 14.1087 13.6749 14.648 13.418L17.456 12.082L15.199 9.945ZM15.224 15.508L13.011 20.158C12.9691 20.2459 12.9065 20.3223 12.8285 20.3806C12.7505 20.4389 12.6594 20.4774 12.5633 20.4926C12.4671 20.5079 12.3686 20.4995 12.2764 20.4682C12.1842 20.4369 12.101 20.3836 12.034 20.313L8.49197 16.574C8.39735 16.4742 8.27131 16.41 8.13497 16.392L3.02797 15.724C2.93149 15.7113 2.83954 15.6753 2.76006 15.6191C2.68058 15.563 2.61596 15.4883 2.57177 15.4016C2.52758 15.3149 2.50514 15.2187 2.5064 15.1214C2.50765 15.0241 2.53256 14.9285 2.57897 14.843L5.04097 10.319C5.10642 10.198 5.12831 10.0582 5.10297 9.923L4.15997 4.86C4.14207 4.76417 4.14778 4.66541 4.17662 4.57229C4.20546 4.47916 4.25656 4.39446 4.3255 4.32553C4.39444 4.25659 4.47913 4.20549 4.57226 4.17665C4.66539 4.14781 4.76414 4.14209 4.85997 4.16L9.92297 5.103C10.0582 5.12834 10.198 5.10645 10.319 5.041L14.843 2.579C14.9286 2.53257 15.0242 2.50769 15.1216 2.50648C15.219 2.50528 15.3152 2.52781 15.4019 2.57211C15.4887 2.61641 15.5633 2.68116 15.6194 2.76076C15.6755 2.84036 15.7114 2.93242 15.724 3.029L16.392 8.135C16.4099 8.27134 16.4742 8.39737 16.574 8.492L20.313 12.034C20.3836 12.101 20.4369 12.1842 20.4682 12.2765C20.4995 12.3687 20.5079 12.4671 20.4926 12.5633C20.4774 12.6595 20.4389 12.7505 20.3806 12.8285C20.3223 12.9065 20.2459 12.9691 20.158 13.011L15.508 15.224C15.3835 15.2832 15.2832 15.3835 15.224 15.508ZM16.021 17.435L17.435 16.021L21.678 20.263L20.263 21.678L16.021 17.435Z'> </path>
            </g>
        </svg>Explore batch inference examples

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
- :doc:`[Gallery] Serve Examples Gallery </serve/examples>`
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
