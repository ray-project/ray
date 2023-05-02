.. _ref-use-cases:

Ray Use Cases
=============

This page indexes common Ray use cases for scaling ML.
It contains highlighted references to blogs, examples, and tutorials also located
elsewhere in the Ray documentation.

.. _ref-use-cases-llm:

LLMs and Gen AI
---------------

Large language models (LLMs) and generative AI are rapidly changing industries, and demand compute at an astonishing pace. Ray provides a distributed compute framework for scaling these models, allowing developers to train and deploy models faster and more efficiently. With specialized libraries for data streaming, training, fine-tuning, hyperparameter tuning, and serving, Ray simplifies the process of developing and deploying large-scale AI models.

.. figure:: /images/llm-stack.png

Learn more about how Ray scales LLMs and generative AI with the following resources.

.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto fixed-height-img

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.anyscale.com/blog/ray-common-production-challenges-for-generative-ai-infrastructure
        :type: url
        :text: [Blog] How Ray solves common production challenges for generative AI infrastructure
        :classes: btn-link btn-block stretched-link webCrawler
    
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.anyscale.com/blog/training-175b-parameter-language-models-at-1000-gpu-scale-with-alpa-and-ray
        :type: url
        :text: [Blog] Training 175B Parameter Language Models at 1000 GPU scale with Alpa and Ray
        :classes: btn-link btn-block stretched-link webCrawler
    
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.anyscale.com/blog/faster-stable-diffusion-fine-tuning-with-ray-air
        :type: url
        :text: [Blog] Faster stable diffusion fine-tuning with Ray AIR
        :classes: btn-link btn-block stretched-link webCrawler
    
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.anyscale.com/blog/how-to-fine-tune-and-serve-llms-simply-quickly-and-cost-effectively-using
        :type: url
        :text: [Blog] How to fine tune and serve LLMs simply, quickly and cost effectively using Ray + DeepSpeed + HuggingFace
        :classes: btn-link btn-block stretched-link webCrawler

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.businessinsider.com/openai-chatgpt-trained-on-anyscale-ray-generative-lifelike-ai-models-2022-12
        :type: url
        :text: [Blog] How OpenAI Uses Ray to Train Tools like ChatGPT
        :classes: btn-link btn-block stretched-link chatgpt

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /ray-air/examples/gptj_deepspeed_fine_tuning
        :type: ref
        :text: [Example] GPT-J-6B Fine-Tuning with Ray AIR and DeepSpeed
        :classes: btn-link btn-block stretched-link antServing

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /ray-air/examples/dreambooth_finetuning
        :type: ref
        :text: [Example] Fine-tuning DreamBooth with Ray AIR
        :classes: btn-link btn-block stretched-link rayForward

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /ray-air/examples/stablediffusion_batch_prediction
        :type: ref
        :text: [Example] Stable Diffusion Batch Prediction with Ray AIR
        :classes: btn-link btn-block stretched-link rayForward

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /ray-air/examples/gptj_serving
        :type: ref
        :text: [Example] GPT-J-6B Serving with Ray AIR
        :classes: btn-link btn-block stretched-link webCrawler

.. _ref-use-cases-batch-infer:

Batch Inference
---------------

Batch inference is the process of generating model predictions on a large "batch" of input data.
Ray for batch inference works with any cloud provider and ML framework,
and is fast and cheap for modern deep learning applications.
It scales from single machines to large clusters with minimal code changes.
As a Python-first framework, you can easily express and interactively develop your inference workloads in Ray.
To learn more about running batch inference with Ray, see the :ref:`batch inference guide<batch_inference_home>`.

.. figure:: batch_inference/images/batch_inference.png


.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto fixed-height-img

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /data/batch-inference
        :type: ref
        :text: [User Guide] Batch Inference with Ray Data
        :classes: btn-link btn-block stretched-link
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://github.com/ray-project/ray-educational-materials/blob/main/Computer_vision_workloads/Semantic_segmentation/Scaling_batch_inference.ipynb
        :type: url
        :text: [Tutorial] Architectures for Scalable Batch Inference with Ray
        :classes: btn-link btn-block stretched-link scalableBatchInference
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

Learn more about many model training with the following resources.

.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto fixed-height-img

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.anyscale.com/blog/training-one-million-machine-learning-models-in-record-time-with-ray
        :type: url
        :text: [Blog] Training One Million ML Models in Record Time with Ray
        :classes: btn-link btn-block stretched-link millionModels
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.anyscale.com/blog/many-models-batch-training-at-scale-with-ray-core
        :type: url
        :text: [Blog] Many Models Batch Training at Scale with Ray Core
        :classes: btn-link btn-block stretched-link manyModels
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
        :text: [Example] Batch Training with Ray Data
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

:ref:`Ray Serve <rayserve>` is well suited for model composition, enabling you to build a complex inference service consisting of multiple ML models and business logic all in Python code.

It supports complex `model deployment patterns <https://www.youtube.com/watch?v=mM4hJLelzSw>`_ requiring the orchestration of multiple Ray actors, where different actors provide inference for different models. Serve handles both batch and online inference and can scale to thousands of models in production.

.. figure:: /images/multi_model_serve.png

  Deployment patterns with Ray Serve. (Click image to enlarge.)

Learn more about model serving with the following resources.

.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto fixed-height-img

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

The :ref:`Ray Tune <tune-main>` library enables any parallel Ray workload to be run under a hyperparameter tuning algorithm.

Running multiple hyperparameter tuning experiments is a pattern apt for distributed computing because each experiment is independent of one another. Ray Tune handles the hard bit of distributing hyperparameter optimization and makes available key features such as checkpointing the best result, optimizing scheduling, and specifying search patterns.

.. figure:: /images/tuning_use_case.png

   Distributed tuning with distributed training per trial.

Learn more about the Tune library with the following talks and user guides.

.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto fixed-height-img

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

The :ref:`Ray Train <train-userguides>` library integrates many distributed training frameworks under a simple Trainer API,
providing distributed orchestration and management capabilities out of the box.

In contrast to training many models, model parallelism partitions a large model across many machines for training. Ray Train has built-in abstractions for distributing shards of models and running training in parallel.

.. figure:: /images/model_parallelism.png

  Model parallelism pattern for distributed large model training.

Learn more about the Train library with the following talks and user guides.

.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto fixed-height-img

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

.. figure:: /images/rllib_use_case.png

   Decentralized distributed proximal polixy optimiation (DD-PPO) architecture.

Learn more about reinforcement learning with the following resources.

.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto fixed-height-img

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

`Merlin <https://shopify.engineering/merlin-shopify-machine-learning-platform>`_ is Shopify's ML platform built on Ray. It enables fast-iteration and `scaling of distributed applications <https://www.youtube.com/watch?v=kbvzvdKH7bc>`_ such as product categorization and recommendations.

.. figure:: /images/shopify-workload.png

  Shopify's Merlin architecture built on Ray.

Spotify `uses Ray for advanced applications <https://www.anyscale.com/ray-summit-2022/agenda/sessions/180>`_ that include personalizing content recommendations for home podcasts, and personalizing Spotify Radio track sequencing.

.. figure:: /images/spotify.png

  How Ray ecosystem empowers ML scientists and engineers at Spotify.

The following highlights feature companies leveraging Ray's unified API to build simpler, more flexible ML platforms.

.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto fixed-height-img

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

The following highlights examples utilizing Ray AIR to implement end-to-end ML workflows.

.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto fixed-height-img

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
    :img-top: /images/detection.jpeg

    +++
    .. link-button:: /ray-air/examples/torch_detection
        :type: ref
        :text: [Example] Object detection with Ray
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

The following highlights feature projects leveraging Ray Core's distributed APIs to simplify the orchestration of large scale workloads.

.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto fixed-height-img

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

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.anyscale.com/blog/ray-breaks-the-usd1-tb-barrier-as-the-worlds-most-cost-efficient-sorting
        :type: url
        :text: [Blog] A new world record on the CloudSort benchmark using Ray
        :classes: btn-link btn-block stretched-link rayForward

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /ray-core/examples/web-crawler
        :type: ref
        :text: [Example] Speed up your web crawler by parallelizing it with Ray
        :classes: btn-link btn-block stretched-link webCrawler
