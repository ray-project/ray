.. _ref-use-cases:

Ray Use Cases
=============

This page indexes common Ray use cases for scaling ML. It contains highlighted references to blogs, examples, and tutorials also located elsewhere in the Ray documentation.

You can filter our use cases by the framework you are using, the use case category,
and the type of workload you are running.
You can select (and deselect) multiple values:

.. raw:: html

    <!--Deselect all-->
    <div>
        <div id="allButton" type="button" class="tag btn btn-primary">All</div>

        <!--Frameworks-->
        <div type="button" class="tag btn btn-outline-primary">PyTorch</div>
        <div type="button" class="tag btn btn-outline-primary">TensorFlow</div>
        <div type="button" class="tag btn btn-outline-primary">XGBoost</div>
        <div type="button" class="tag btn btn-outline-primary">LightGBM</div>
        <div type="button" class="tag btn btn-outline-primary">Sklearn</div>

        <!--Domains-->
        <div type="button" class="tag btn btn-outline-primary">Classification</div>
        <div type="button" class="tag btn btn-outline-primary">Regression</div>
        <div type="button" class="tag btn btn-outline-primary">Object Detection</div>
        <div type="button" class="tag btn btn-outline-primary">Image Segmentation</div>
        <div type="button" class="tag btn btn-outline-primary">Reinforcement Learning</div>

        <!--Components-->
        <div type="button" class="tag btn btn-outline-primary">Preprocessing</div>
        <div type="button" class="tag btn btn-outline-primary">Training</div>
        <div type="button" class="tag btn btn-outline-primary">Tuning</div>
        <div type="button" class="tag btn btn-outline-primary">Prediction</div>
        <div type="button" class="tag btn btn-outline-primary">Serving</div>
    </div>


Batch Inference
---------------

Batch inference refers to generating model predictions over a set of input observations. The model could be a regression model, neural network, or simply a Python function. Ray can scale batch inference from single GPU machines to large clusters.

.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://github.com/ray-project/ray-educational-materials/blob/main/Computer_vision_workloads/Semantic_segmentation/Scaling_batch_inference.ipynb
        :type: url
        :text: [Tutorial] Architectures for Scalable Batch Inference with Ray
        :classes: btn-link btn-block stretched-link scalableBatchInference
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.anyscale.com/blog/model-batch-inference-in-ray-actors-actorpool-and-datasets
        :type: url
        :text: [Blog] Batch Inference in Ray: Actors, ActorPool, and Datasets
        :classes: btn-link btn-block stretched-link batchActorPool
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /ray-core/examples/batch_prediction
        :type: ref
        :text: [Example] Batch Prediction using Ray Core
        :classes: btn-link btn-block stretched-link batchCore
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /data/examples/nyc_taxi_basic_processing
        :type: ref
        :text: [Example] Batch Inference on NYC taxi data using Ray Data
        :classes: btn-link btn-block stretched-link nycTaxiData

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /data/examples/ocr_example
        :type: ref
        :text: [Example] Batch OCR processing using Ray Data
        :classes: btn-link btn-block stretched-link batchOcr

.. _ref-use-cases-mmt:

Many Model Training
-------------------

Many model training is common in ML use cases such as time series forecasting, which require fitting of models on multiple data batches corresponding to locations, products, etc.
Here, the focus is on training many models on subsets of a dataset. This is in contrast to training a single model on the entire dataset.

How do I do many model training on Ray?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are three ways of using Ray to express this workload.

1. If you have a large amount of data, use Ray Data (:ref:`Tutorial <mmt-datasets>`).
2. If you have a small amount of data (<10GB), want to integrate with tools, such as wandb and mlflow, and you have less than 20,000 models, use Ray Tune (:ref:`Tutorial <mmt-tune>`).
3. If your use case does not fit in any of the above categories, for example if you need to scale up to 1 million models, use Ray Core (:ref:`Tutorial <mmt-core>`), which gives you finer-grained control over the application. However, note that this is for advanced users and will require understanding of Ray Core :ref:`design patterns and anti-patterns <core-patterns>`.

.. TODO
  Add link to many model training blog.

.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.anyscale.com/blog/training-one-million-machine-learning-models-in-record-time-with-ray
        :type: url
        :text: [Blog] Training One Million ML Models in Record Time with Ray
        :classes: btn-link btn-block stretched-link millionModels
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /ray-core/examples/batch_training
        :type: ref
        :text: [Example] Batch Training with Ray Core
        :classes: btn-link btn-block stretched-link batchTrainingCore
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /data/examples/batch_training
        :type: ref
        :text: [Example] Batch Training with Ray Datasets
        :classes: btn-link btn-block stretched-link batchTrainingDatasets
    ---
    :img-top: /images/tune.png

    .. link-button:: /tune/tutorials/tune-run
        :type: ref
        :text: [Guide] Tune Basic Parallel Experiments
        :classes: btn-link btn-block stretched-link tuneBasicParallel
    ---
    :img-top: /images/tune.png

    .. link-button:: /ray-air/examples/batch_tuning
        :type: ref
        :text: [Example] Batch Training and Tuning using Ray Tune
        :classes: btn-link btn-block stretched-link tuneBatch
    ---
    :img-top: /images/carrot.png

    .. link-button:: https://www.youtube.com/watch?v=3t26ucTy0Rs
        :type: url
        :text: [Talk] Scaling Instacart fulfillment ML on Ray
        :classes: btn-link btn-block stretched-link instacartFulfillment

Model Serving
-------------

Ray's official serving solution is Ray Serve.
Ray Serve is particularly well suited for model composition, enabling you to build a complex inference service consisting of multiple ML models and business logic all in Python code.


.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto

    ---
    :img-top: /images/serve.svg

    .. link-button:: https://www.youtube.com/watch?v=UtH-CMpmxvI
        :type: url
        :text: [Talk] Productionizing ML at Scale with Ray Serve
        :classes: btn-link btn-block stretched-link productionizingMLServe
    ---
    :img-top: /images/serve.svg

    .. link-button:: https://www.anyscale.com/blog/simplify-your-mlops-with-ray-and-ray-serve
        :type: url
        :text: [Blog] Simplify your MLOps with Ray & Ray Serve
        :classes: btn-link btn-block stretched-link simplifyMLOpsServe
    ---
    :img-top: /images/serve.svg

    .. link-button:: /serve/getting_started
        :type: ref
        :text: [Guide] Getting Started with Ray Serve
        :classes: btn-link btn-block stretched-link gettingStartedServe
    ---
    :img-top: /images/serve.svg

    .. link-button:: /serve/model_composition
        :type: ref
        :text: [Guide] Model Composition in Serve
        :classes: btn-link btn-block stretched-link compositionServe
    ---
    :img-top: /images/grid.png

    .. link-button:: /serve/tutorials/index
        :type: ref
        :text: [Gallery] Serve Examples Gallery
        :classes: btn-link btn-block stretched-link examplesServe
    ---
    :img-top: /images/grid.png

    .. link-button:: https://www.anyscale.com/blog?tag=ray_serve
        :type: url
        :text: [Gallery] More Serve Use Cases on the Blog
        :classes: btn-link btn-block stretched-link useCasesServe

Hyperparameter Tuning
---------------------

Ray's Tune library enables any parallel Ray workload to be run under a hyperparameter tuning algorithm.
Learn more about the Tune library with the following talks and user guides.

.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto

    ---
    :img-top: /images/tune.png

    .. link-button:: /tune/getting-started
        :type: ref
        :text: [Guide] Getting Started with Ray Tune
        :classes: btn-link btn-block stretched-link gettingStartedTune
    ---
    :img-top: /images/tune.png

    .. link-button:: https://www.anyscale.com/blog/how-to-distribute-hyperparameter-tuning-using-ray-tune
        :type: url
        :text: [Blog] How to distribute hyperparameter tuning with Ray Tune
        :classes: btn-link btn-block stretched-link distributeHPOTune
    ---
    :img-top: /images/tune.png

    .. link-button:: https://www.youtube.com/watch?v=KgYZtlbFYXE
        :type: url
        :text: [Talk] Simple Distributed Hyperparameter Optimization
        :classes: btn-link btn-block stretched-link simpleDistributedHPO
    ---
    :img-top: /images/tune.png

    .. link-button:: https://www.anyscale.com/blog/hyperparameter-search-hugging-face-transformers-ray-tune
        :type: url
        :text: [Blog] Hyperparameter Search with 🤗 Transformers
        :classes: btn-link btn-block stretched-link HPOTransformers
    ---
    :img-top: /images/grid.png

    .. link-button:: /tune/examples/index
        :type: ref
        :text: [Gallery] Ray Tune Examples Gallery
        :classes: btn-link btn-block stretched-link examplesTune
    ---
    :img-top: /images/grid.png

    .. link-button:: https://www.anyscale.com/blog?tag=ray-tune
        :type: url
        :text: More Tune use cases on the Blog
        :classes: btn-link btn-block stretched-link useCasesTune

Distributed Training
--------------------

Ray's Train library integrates many distributed training frameworks under a simple Trainer API,
providing distributed orchestration and management capabilities out of the box.
Learn more about the Train library with the following talks and user guides.

.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.youtube.com/watch?v=e-A93QftCfc
        :type: url
        :text: [Talk] Ray Train, PyTorch, TorchX, and distributed deep learning
        :classes: btn-link btn-block stretched-link pyTorchTrain
    ---
    :img-top: /images/uber.png

    .. link-button:: https://www.uber.com/blog/elastic-xgboost-ray/
        :type: url
        :text: [Blog] Elastic Distributed Training with XGBoost on Ray
        :classes: btn-link btn-block stretched-link xgboostTrain
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /train/train
        :type: ref
        :text: [Guide] Getting Started with Ray Train
        :classes: btn-link btn-block stretched-link gettingStartedTrain
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /ray-air/examples/huggingface_text_classification
        :type: ref
        :text: [Example] Fine-tune a 🤗 Transformers model
        :classes: btn-link btn-block stretched-link trainingTransformers
    ---
    :img-top: /images/grid.png

    .. link-button:: /train/examples
        :type: ref
        :text: [Gallery] Ray Train Examples Gallery
        :classes: btn-link btn-block stretched-link examplesTrain
    ---
    :img-top: /images/grid.png

    .. link-button:: https://www.anyscale.com/blog?tag=ray_train
        :type: url
        :text: [Gallery] More Train Use Cases on the Blog
        :classes: btn-link btn-block stretched-link useCasesTrain

Reinforcement Learning
----------------------

RLlib is an open-source library for reinforcement learning (RL), offering support for production-level, highly distributed RL workloads while maintaining unified and simple APIs for a large variety of industry applications. RLlib is used by industry leaders in many different verticals, such as climate control, industrial control, manufacturing and logistics, finance, gaming, automobile, robotics, boat design, and many others.

.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto

    ---
    :img-top: /rllib/images/rllib-logo.png

    .. link-button:: https://applied-rl-course.netlify.app/
        :type: url
        :text: [Course] Applied Reinforcement Learning with RLlib
        :classes: btn-link btn-block stretched-link appliedRLCourse
    ---
    :img-top: /rllib/images/rllib-logo.png

    .. link-button:: https://medium.com/distributed-computing-with-ray/intro-to-rllib-example-environments-3a113f532c70
        :type: url
        :text: [Blog] Intro to RLlib: Example Environments
        :classes: btn-link btn-block stretched-link introRLlib
    ---
    :img-top: /rllib/images/rllib-logo.png

    .. link-button:: /rllib/rllib-training
        :type: ref
        :text: [Guide] Getting Started with RLlib
        :classes: btn-link btn-block stretched-link gettingStartedRLlib
    ---
    :img-top: /images/riot.png

    .. link-button:: https://www.anyscale.com/events/2022/03/29/deep-reinforcement-learning-at-riot-games
        :type: url
        :text: [Talk] Deep reinforcement learning at Riot Games
        :classes: btn-link btn-block stretched-link riotRL
    ---
    :img-top: /images/grid.png

    .. link-button:: /rllib/rllib-examples
        :type: ref
        :text: [Gallery] RLlib Examples Gallery
        :classes: btn-link btn-block stretched-link examplesRL
    ---
    :img-top: /images/grid.png

    .. link-button:: https://www.anyscale.com/blog?tag=rllib
        :type: url
        :text: [Gallery] More RL Use Cases on the Blog
        :classes: btn-link btn-block stretched-link useCasesRL

ML Platform
-----------

The following highlights feature companies leveraging Ray's unified API to build simpler, more flexible ML platforms.

.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto

    ---
    :img-top: /images/shopify.png

    .. link-button:: https://shopify.engineering/merlin-shopify-machine-learning-platform
        :type: url
        :text: [Blog] The Magic of Merlin - Shopify's New ML Platform
        :classes: btn-link btn-block stretched-link merlin
    ---
    :img-top: /images/uber.png

    .. link-button:: https://drive.google.com/file/d/1BS5lfXfuG5bnI8UM6FdUrR7CiSuWqdLn/view
        :type: url
        :text: [Slides] Large Scale Deep Learning Training and Tuning with Ray
        :classes: btn-link btn-block stretched-link uberScaleDL
    ---
    :img-top: /images/carrot.png

    .. link-button:: https://www.instacart.com/company/how-its-made/griffin-how-instacarts-ml-platform-tripled-ml-applications-in-a-year/
        :type: url
        :text: [Blog] Griffin: How Instacart’s ML Platform Tripled in a year
        :classes: btn-link btn-block stretched-link instacartMLPlatformTripled
    ---
    :img-top: /images/predibase.png

    .. link-button:: https://www.youtube.com/watch?v=B5v9B5VSI7Q
        :type: url
        :text: [Talk] Predibase - A low-code deep learning platform built for scale
        :classes: btn-link btn-block stretched-link predibase
    ---
    :img-top: /images/gke.png

    .. link-button:: https://cloud.google.com/blog/products/ai-machine-learning/build-a-ml-platform-with-kubeflow-and-ray-on-gke
        :type: url
        :text: [Blog] Building a ML Platform with Kubeflow and Ray on GKE
        :classes: btn-link btn-block stretched-link GKEMLPlatform
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.youtube.com/watch?v=_L0lsShbKaY
        :type: url
        :text: [Talk] Ray Summit Panel - ML Platform on Ray
        :classes: btn-link btn-block stretched-link summitMLPlatform


End-to-End ML Workflows
-----------------------

The following are highlighted examples utilizing Ray AIR to implement end-to-end ML workflows.

.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto

    ---
    :img-top: /images/text-classification.png

    .. link-button:: /ray-air/examples/huggingface_text_classification
        :type: ref
        :text: [Example] Text classification with Ray
        :classes: btn-link btn-block stretched-link trainingTransformers
    ---
    :img-top: /images/image-classification.webp

    .. link-button:: /ray-air/examples/torch_image_example
        :type: ref
        :text: [Example] Image classification with Ray
        :classes: btn-link btn-block stretched-link torchImageExample
    ---
    :img-top: /images/credit.png

    .. link-button:: /ray-air/examples/feast_example
        :type: ref
        :text: [Example] Credit scoring with Ray and Feast
        :classes: btn-link btn-block stretched-link feastExample
    ---
    :img-top: /images/tabular-data.png

    .. link-button:: /ray-air/examples/xgboost_example
        :type: ref
        :text: [Example] Machine learning on tabular data
        :classes: btn-link btn-block stretched-link xgboostExample
    ---
    :img-top: /images/timeseries.png

    .. link-button:: /ray-core/examples/automl_for_time_series
        :type: ref
        :text: [Example] AutoML for Time Series with Ray
        :classes: btn-link btn-block stretched-link timeSeriesAutoML
    ---
    :img-top: /images/grid.png

    .. link-button:: /ray-air/examples/index
        :type: ref
        :text: [Gallery] Full Ray AIR Examples Gallery
        :classes: btn-link btn-block stretched-link AIRExamples

Large Scale Workload Orchestration
----------------------------------

The following highlights feature companies leveraging Ray Core's distributed APIs to simplify the orchestration of large scale workloads.

.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://archive.ph/o/aNvFN/https://www.businessinsider.com/openai-chatgpt-trained-on-anyscale-ray-generative-lifelike-ai-models-2022-12
        :type: url
        :text: [Blog] How OpenAI Uses Ray to Train Tools like ChatGPT
        :classes: btn-link btn-block stretched-link chatgpt
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.anyscale.com/blog/building-highly-available-and-scalable-online-applications-on-ray-at-ant
        :type: url
        :text: [Blog] Highly Available and Scalable Online Applications on Ray at Ant Group
        :classes: btn-link btn-block stretched-link antServing

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.anyscale.com/blog/ray-forward-2022
        :type: url
        :text: [Blog] Ray Forward 2022 Conference: Hyper-scale Ray Application Use Cases
        :classes: btn-link btn-block stretched-link rayForward


Basic Examples
--------------

.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /ray-core/examples/web-crawler
        :type: ref
        :text: Speed up your web crawler by parallelizing it with Ray
        :classes: btn-link btn-block stretched-link webCrawler
