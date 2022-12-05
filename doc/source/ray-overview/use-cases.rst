.. _ref-use-cases:

Ray Use Cases
=============

This page indexes common Ray use cases for scaling ML. It contains highlighted references to blogs, examples, and tutorials also located elsewhere in the Ray documentation.

Batch Inference
---------------

Batch inference refers to generating model predictions over a set of input observations. The model could be a regression model, neural network, or simply a Python function. Ray can scale batch inference from single GPU machines to large clusters.

.. panels::
    :container: container pb-4
    :column: col-md-4 px-2 py-2
    :img-top-cls: pt-5 w-75 d-block mx-auto

    ---
    :img-top: /images/ray_logo.png

    +++
    .. link-button:: https://github.com/ray-project/ray-educational-materials/blob/main/Computer_vision_workloads/Semantic_segmentation/Scaling_batch_inference.ipynb
        :type: url
        :text: [Tutorial] Architectures for Scalable Batch Inference with Ray
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/ray_logo.png

    +++
    .. link-button:: https://www.anyscale.com/blog/model-batch-inference-in-ray-actors-actorpool-and-datasets
        :type: url
        :text: [Blog] Batch Inference in Ray: Actors, ActorPool, and Datasets
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/ray_logo.png

    +++
    .. link-button:: /ray-core/examples/batch_prediction
        :type: ref
        :text: [Example] Batch Prediction using Ray Core
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/ray_logo.png

    +++
    .. link-button:: /data/examples/nyc_taxi_basic_processing
        :type: ref
        :text: [Example] Batch Inference on NYC taxi data using Ray Data
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/ray_logo.png

    +++
    .. link-button:: /data/examples/ocr_example
        :type: ref
        :text: [Example] Batch OCR processing using Ray Data
        :classes: btn-link btn-block stretched-link

Many Model Training
-------------------

Many model training is common in ML use cases such as time series forecasting, which require fitting of models on multiple data batches corresponding to locations, products, etc.
Here, the focus is on training many models on subsets of a dataset. This is in contrast to training a single model on the entire dataset.

.. TODO
  Add link to many model training blog.

.. panels::
    :container: container pb-4
    :column: col-md-4 px-2 py-2
    :img-top-cls: pt-5 w-75 d-block mx-auto

    ---
    :img-top: /images/ray_logo.png

    +++
    .. link-button:: /ray-core/examples/batch_training
        :type: ref
        :text: [Example] Batch Training with Ray Core
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/ray_logo.png

    +++
    .. link-button:: /data/examples/batch_training
        :type: ref
        :text: [Example] Batch Training with Ray Datasets
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/tune.png

    +++
    .. link-button:: /tune/tutorials/tune-run
        :type: ref
        :text: [Guide] Tune Basic Parallel Experiments
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/tune.png

    +++
    .. link-button:: /ray-air/examples/batch_tuning
        :type: ref
        :text: [Example] Batch Training and Tuning using Ray Tune
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/carrot.png

    +++
    .. link-button:: https://www.youtube.com/watch?v=3t26ucTy0Rs
        :type: url
        :text: [Talk] Scaling Instacart fulfillment ML on Ray
        :classes: btn-link btn-block stretched-link

Model Serving
-------------

Ray's official serving solution is Ray Serve.
Ray Serve is particularly well suited for model composition, enabling you to build a complex inference service consisting of multiple ML models and business logic all in Python code.


.. panels::
    :container: container pb-4
    :column: col-md-4 px-2 py-2
    :img-top-cls: pt-5 w-75 d-block mx-auto

    ---
    :img-top: /images/serve.svg

    +++
    .. link-button:: https://www.youtube.com/watch?v=UtH-CMpmxvI
        :type: url
        :text: [Talk] Productionizing ML at Scale with Ray Serve
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/serve.svg

    +++
    .. link-button:: https://www.anyscale.com/blog/simplify-your-mlops-with-ray-and-ray-serve
        :type: url
        :text: [Blog] Simplify your MLOps with Ray & Ray Serve
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/serve.svg

    +++
    .. link-button:: https://www.anyscale.com/blog/ray-serve-fastapi-the-best-of-both-worlds
        :type: url
        :text: [Blog] Ray Serve + FastAPI: The best of both worlds
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/serve.svg

    +++
    .. link-button:: /serve/getting_started
        :type: ref
        :text: [Guide] Getting Started with Ray Serve
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/serve.svg

    +++
    .. link-button:: /serve/model_composition
        :type: ref
        :text: [Guide] Model Composition in Serve
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/grid.png

    +++
    .. link-button:: /serve/tutorials/index
        :type: ref
        :text: [Gallery] Serve Examples Gallery
        :classes: btn-link btn-block stretched-link

Hyperparameter Tuning
---------------------

Ray's Tune library enables any parallel Ray workload to be run under a hyperparameter tuning algorithm.
Learn more about the Tune library with the following talks and user guides.

.. panels::
    :container: container pb-4
    :column: col-md-4 px-2 py-2
    :img-top-cls: pt-5 w-75 d-block mx-auto

    ---
    :img-top: /images/tune.png

    +++
    .. link-button:: /tune/getting-started
        :type: ref
        :text: [Guide] Getting Started with Ray Tune
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/tune.png

    +++
    .. link-button:: https://www.anyscale.com/blog/how-to-distribute-hyperparameter-tuning-using-ray-tune
        :type: url
        :text: [Blog] How to distribute hyperparameter tuning with Ray Tune
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/tune.png

    +++
    .. link-button:: https://www.youtube.com/watch?v=KgYZtlbFYXE
        :type: url
        :text: [Talk] Simple Distributed Hyperparameter Optimization
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/tune.png

    +++
    .. link-button:: https://www.anyscale.com/blog/hyperparameter-search-hugging-face-transformers-ray-tune
        :type: url
        :text: [Blog] Hyperparameter Search with 🤗 Transformers
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/tune.png

    +++
    .. link-button:: /ray-air/examples/automl_with_ray_air
        :type: ref
        :text: [Example] Simple AutoML for time series with Ray AIR
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/grid.png

    +++
    .. link-button:: /tune/examples/index
        :type: ref
        :text: [Gallery] Ray Tune Examples Gallery
        :classes: btn-link btn-block stretched-link

Distributed Training
--------------------

Ray's Train library integrates many distributed training frameworks under a simple Trainer API,
providing distributed orchestration and management capabilities out of the box.
Learn more about the Train library with the following talks and user guides.

.. panels::
    :container: container pb-4
    :column: col-md-4 px-2 py-2
    :img-top-cls: pt-5 w-75 d-block mx-auto

    ---
    :img-top: /images/ray_logo.png

    +++
    .. link-button:: https://www.youtube.com/watch?v=e-A93QftCfc
        :type: url
        :text: [Talk] Ray Train, PyTorch, TorchX, and distributed deep learning
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/uber.png

    +++
    .. link-button:: https://www.uber.com/blog/horovod-ray/
        :type: url
        :text: [Blog] Elastic Deep Learning with Horovod on Ray
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/uber.png

    +++
    .. link-button:: https://www.uber.com/blog/elastic-xgboost-ray/
        :type: url
        :text: [Blog] Elastic Distributed Training with XGBoost on Ray
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/ray_logo.png

    +++
    .. link-button:: /train/train
        :type: ref
        :text: [Guide] Getting Started with Ray Train
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/ray_logo.png

    +++
    .. link-button:: /ray-air/examples/huggingface_text_classification
        :type: ref
        :text: [Example] Fine-tune a 🤗 Transformers model
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/grid.png

    +++
    .. link-button:: /train/examples
        :type: ref
        :text: [Gallery] Ray Train Examples Gallery
        :classes: btn-link btn-block stretched-link

Reinforcement Learning
----------------------

RLlib is an open-source library for reinforcement learning (RL), offering support for production-level, highly distributed RL workloads while maintaining unified and simple APIs for a large variety of industry applications. RLlib is used by industry leaders in many different verticals, such as climate control, industrial control, manufacturing and logistics, finance, gaming, automobile, robotics, boat design, and many others.

.. panels::
    :container: container pb-4
    :column: col-md-4 px-2 py-2
    :img-top-cls: pt-5 w-75 d-block mx-auto

    ---
    :img-top: /rllib/images/rllib-logo.png

    +++
    .. link-button:: https://applied-rl-course.netlify.app/
        :type: url
        :text: [Course] Applied Reinforcement Learning with RLlib
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /rllib/images/rllib-logo.png

    +++
    .. link-button:: https://medium.com/distributed-computing-with-ray/intro-to-rllib-example-environments-3a113f532c70
        :type: url
        :text: [Blog] Intro to RLlib: Example Environments
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /rllib/images/rllib-logo.png

    +++
    .. link-button:: /rllib/rllib-training
        :type: ref
        :text: [Guide] Getting Started with RLlib
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /rllib/images/rllib-logo.png

    +++
    .. link-button:: https://deumbra.com/2022/08/rllib-for-deep-hierarchical-multiagent-reinforcement-learning/
        :type: url
        :text: [Blog] RLlib for Deep Hierarchical Multiagent Reinforcement Learning
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/riot.png

    +++
    .. link-button:: https://www.anyscale.com/events/2022/03/29/deep-reinforcement-learning-at-riot-games
        :type: url
        :text: [Talk] Deep reinforcement learning at Riot Games
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/grid.png

    +++
    .. link-button:: /rllib/rllib-examples
        :type: ref
        :text: [Gallery] RLlib Examples Gallery
        :classes: btn-link btn-block stretched-link


ML Platform
-----------

The following highlights feature companies leveraging Ray's unified API to build simpler, more flexible ML platforms.

.. panels::
    :container: container pb-4
    :column: col-md-4 px-2 py-2
    :img-top-cls: pt-5 w-75 d-block mx-auto

    ---
    :img-top: /images/shopify.png

    +++
    .. link-button:: https://shopify.engineering/merlin-shopify-machine-learning-platform
        :type: url
        :text: [Blog] The Magic of Merlin - Shopify's New ML Platform
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/uber.png

    +++
    .. link-button:: https://drive.google.com/file/d/1BS5lfXfuG5bnI8UM6FdUrR7CiSuWqdLn/view
        :type: url
        :text: [Slides] Large Scale Deep Learning Training and Tuning with Ray
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/carrot.png

    +++
    .. link-button:: https://www.instacart.com/company/how-its-made/griffin-how-instacarts-ml-platform-tripled-ml-applications-in-a-year/
        :type: url
        :text: [Blog] Griffin: How Instacart’s ML Platform Tripled in a year
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/predibase.png

    +++
    .. link-button:: https://www.youtube.com/watch?v=B5v9B5VSI7Q
        :type: url
        :text: [Talk] Predibase - A low-code deep learning platform built for scale
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/gke.png

    +++
    .. link-button:: https://cloud.google.com/blog/products/ai-machine-learning/build-a-ml-platform-with-kubeflow-and-ray-on-gke
        :type: url
        :text: [Blog] Building a ML Platform with Kubeflow and Ray on GKE
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/ray_logo.png

    +++
    .. link-button:: https://www.youtube.com/watch?v=_L0lsShbKaY
        :type: url
        :text: [Talk] Ray Summit Panel - ML Platform on Ray
        :classes: btn-link btn-block stretched-link

End-to-End ML Workflows
-----------------------

The following are highlighted examples utilizing Ray AIR to implement end-to-end ML workflows.

.. panels::
    :container: container pb-4
    :column: col-md-4 px-2 py-2
    :img-top-cls: pt-5 w-75 d-block mx-auto

    ---
    :img-top: /images/text-classification.png

    +++
    .. link-button:: /ray-air/examples/huggingface_text_classification
        :type: ref
        :text: [Example] Text classification with Ray
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/image-classification.webp

    +++
    .. link-button:: /ray-air/examples/torch_image_example
        :type: ref
        :text: [Example] Image classification with Ray
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/credit.png

    +++
    .. link-button:: /ray-air/examples/feast_example
        :type: ref
        :text: [Example] Credit scoring with Ray and Feast
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/tabular-data.png

    +++
    .. link-button:: /ray-air/examples/xgboost_example
        :type: ref
        :text: [Example] Machine learning on tabular data
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/timeseries.png

    +++
    .. link-button:: /ray-core/examples/automl_for_time_series
        :type: ref
        :text: [Example] AutoML for Time Series with Ray
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/grid.png

    +++
    .. link-button:: /ray-air/examples/index
        :type: ref
        :text: [Gallery] Full Ray AIR Examples Gallery
        :classes: btn-link btn-block stretched-link
