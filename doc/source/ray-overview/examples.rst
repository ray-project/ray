.. _ref-ray-examples:

.. title:: Ray Example Gallery

.. raw:: html

    <div class="searchWrap">
       <div class="searchDiv">
          <input type="text" id="searchInput" class="searchTerm"
           placeholder="What are you looking for? (Ex.: PyTorch, Tune, RL)">
          <button id="filterButton" type="button" class="searchButton">
            <i class="fa fa-search"></i>
         </button>
       </div>
    </div>

..
    NOTE: available tags for the grid-item-card (double-check with tags.js):
    Use cases: llm, cv, ts, nlp, rl
    ML Workloads: data-processing, training, tuning, serving, inference
    MLOps: tracking, monitoring
    Frameworks: pytorch, tensorflow


.. grid:: 1 2 2 2
    :gutter: 1
    :class-container: gallery-container container pb-3

    .. grid-item-card:: :bdg-primary:`Blog`
        :class-item: gallery-item

        .. button-link:: https://www.anyscale.com/blog/ray-common-production-challenges-for-generative-ai-infrastructure

            How Ray solves common production challenges for generative AI infrastructure

    .. grid-item-card:: :bdg-primary:`Blog`
        :class-item: gallery-item training llm nlp

        .. button-link:: https://www.anyscale.com/blog/training-175b-parameter-language-models-at-1000-gpu-scale-with-alpa-and-ray

            Training 175B Parameter Language Models at 1000 GPU scale with Alpa and Ray

    .. grid-item-card:: :bdg-primary:`Blog`
        :class-item: gallery-item

        .. button-link:: https://www.anyscale.com/blog/faster-stable-diffusion-fine-tuning-with-ray-air

            Faster stable diffusion fine-tuning with Ray AIR

    .. grid-item-card:: :bdg-primary:`Blog`
        :class-item: gallery-item tuning serving

        .. button-link:: https://www.anyscale.com/blog/how-to-fine-tune-and-serve-llms-simply-quickly-and-cost-effectively-using

            How to fine tune and serve LLMs simply, quickly and cost effectively using Ray + DeepSpeed + HuggingFace

    .. grid-item-card:: :bdg-primary:`Blog`
        :class-item: gallery-item

        .. button-link:: https://www.businessinsider.com/openai-chatgpt-trained-on-anyscale-ray-generative-lifelike-ai-models-2022-12

            How OpenAI Uses Ray to Train Tools like ChatGPT

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item

        .. button-ref:: /ray-air/examples/gptj_deepspeed_fine_tuning

            GPT-J-6B Fine-Tuning with Ray AIR and DeepSpeed

    .. grid-item-card:: :bdg-success:`Tutorial`
        :class-item: gallery-item pytorch

        .. button-ref:: /ray-air/examples/convert_existing_pytorch_code_to_ray_air

            Get started with Ray AIR from an existing PyTorch codebase

    .. grid-item-card:: :bdg-success:`Tutorial`
        :class-item: gallery-item tensorflow

        .. button-ref:: /ray-air/examples/convert_existing_tf_code_to_ray_air

            Get started with Ray AIR from an existing Tensorflow/Keras

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item training

        .. button-ref:: /ray-air/examples/lightgbm_example

            Distributed training with LightGBM

    .. grid-item-card:: :bdg-success:`Tutorial`
        :class-item: gallery-item training

        .. button-ref:: /ray-air/examples/xgboost_example

            Distributed training with XGBoost

    .. grid-item-card:: :bdg-success:`Tutorial`
        :class-item: gallery-item

        .. button-ref:: /ray-air/examples/analyze_tuning_results

            Distributed tuning with XGBoost

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item

        .. button-ref:: /ray-air/examples/sklearn_example

            Integrating with Scikit-Learn (non-distributed)

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item ts

        .. button-ref:: /ray-air/examples/automl_with_ray_air

            Build an AutoML system for time-series forecasting with Ray AIR

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item data-processing

        .. button-ref:: /ray-air/examples/batch_tuning

            Perform batch tuning on NYC Taxi Dataset with Ray AIR

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item data-processing

        .. button-ref:: /ray-air/examples/batch_forecasting

            Perform batch forecasting on NYC Taxi Dataset with Prophet, ARIMA and Ray AIR

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item

        .. button-ref:: /ray-air/examples/huggingface_text_classification

            How to use Ray AIR to run Hugging Face Transformers fine-tuning on a text classification task

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item

        .. button-ref:: /ray-air/examples/gptj_deepspeed_fine_tuning

            How to use Ray AIR to run Hugging Face Transformers with DeepSpeed for fine-tuning a large model

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item

        .. button-ref:: /ray-air/examples/gptj_batch_prediction

            How to use Ray AIR to do batch prediction with the Hugging Face Transformers GPT-J model

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item serving

        .. button-ref:: /ray-air/examples/gptj_serving

            How to use Ray AIR to do online serving with the Hugging Face Transformers GPT-J model

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item cv tuning

        .. button-ref:: /ray-air/examples/dreambooth_finetuning

            How to fine-tune a DreamBooth text-to-image model with your own images.

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item data-processing inference

        .. button-ref:: /ray-air/examples/opt_deepspeed_batch_inference

            How to run batch inference on a dataset of texts with a 30B OPT model

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-ref:: /ray-air/examples/dolly_lightning_fsdp_finetuning

            How to fine-tune a dolly-v2-7b model with Ray AIR LightningTrainer and FSDP

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item pytorch cv

        .. button-ref:: /ray-air/examples/torch_image_example

            Torch Image Classification Example with Ray AIR

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item pytorch cv

        .. button-ref:: /ray-air/examples/torch_detection

            Torch Object Detection Example with Ray AIR

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item pytorch

        .. button-ref:: /ray-air/examples/pytorch_resnet_batch_prediction

            Torch Batch Prediction Example with Ray AIR

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item cv

        .. button-ref:: /ray-air/examples/stablediffusion_batch_prediction

            How to use Ray AIR to do batch prediction with the Stable Diffusion text-to-image model

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item

        .. button-ref:: /ray-air/examples/upload_to_comet_ml

            How to log results and upload models to Comet ML

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item

        .. button-ref:: /ray-air/examples/upload_to_wandb

            How to log results and upload models to Weights and Biases

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item serving

        .. button-ref:: /ray-air/examples/rl_serving_example

            Serving RL models with Ray AIR

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item

        .. button-ref:: /ray-air/examples/rl_online_example

            RL Online Learning with Ray AIR

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item

        .. button-ref:: /ray-air/examples/rl_offline_example

            RL Offline Learning with Ray AIR

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item pytorch

        .. button-ref:: /ray-air/examples/torch_incremental_learning

            Incrementally train and deploy a PyTorch CV model

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item training inference

        .. button-ref:: /ray-air/examples/feast_example

            Integrate with Feast feature store in both train and inference

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item pytorch tensorflow serving

        .. button-ref:: /serve/tutorials/serve-ml-models

            Serving ML models with Ray Serve (Tensorflow, PyTorch, Scikit-Learn, others)

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item serving

        .. button-ref:: /serve/tutorials/batch

            Batching tutorial for Ray Serve

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item rl serving

        .. button-ref:: /serve/tutorials/rllib

            Serving RLlib Models with Ray Serve

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item serving

        .. button-ref:: /serve/tutorials/gradio-integration

            Scaling your Gradio app with Ray Serve

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item serving

        .. button-ref:: /serve/tutorials/gradio-dag-visualization

            Visualizing a Deployment Graph with Gradio

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item serving

        .. button-ref:: /serve/tutorials/java

            Java tutorial for Ray Serve

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item serving

        .. button-ref:: /serve/tutorials/stable-diffusion

            Serving a Stable Diffusion Model

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item serving

        .. button-ref:: /serve/tutorials/text-classification

            Serving a Distilbert Model

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item cv serving

        .. button-ref:: /serve/tutorials/object-detection

            Serving an Object Detection Model

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item

        .. button-ref:: /ray-air/examples/dreambooth_finetuning

            Fine-tuning DreamBooth with Ray AIR

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item

        .. button-ref:: /ray-air/examples/stablediffusion_batch_prediction

            Stable Diffusion Batch Prediction with Ray AIR

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item serving

        .. button-ref:: /ray-air/examples/gptj_serving

            GPT-J-6B Serving with Ray AIR

    .. grid-item-card:: :bdg-primary:`Blog`
        :class-item: gallery-item inference

        .. button-link:: https://www.anyscale.com/blog/offline-batch-inference-comparing-ray-apache-spark-and-sagemaker

            Offline Batch Inference: Comparing Ray, Apache Spark, and SageMaker

    .. grid-item-card:: :bdg-primary:`Blog`
        :class-item: gallery-item

        .. button-link:: https://www.anyscale.com/blog/streaming-distributed-execution-across-cpus-and-gpus

            Streaming distributed execution across CPUs and GPUs

    .. grid-item-card:: :bdg-primary:`Blog`
        :class-item: gallery-item llm nlp data-processing inference

        .. button-link:: https://www.anyscale.com/blog/turbocharge-langchain-now-guide-to-20x-faster-embedding

            Using Ray Data to parallelize LangChain inference

    .. grid-item-card:: :bdg-primary:`Blog`
        :class-item: gallery-item data-processing inference

        .. button-ref:: /data/batch_inference

            Batch Prediction using Ray Data

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item data-processing inference

        .. button-ref:: /data/examples/nyc_taxi_basic_processing

            Batch Inference on NYC taxi data using Ray Data

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item data-processing inference

        .. button-ref:: /data/examples/ocr_example

            Batch OCR processing using Ray Data

    .. grid-item-card:: :bdg-primary:`Blog`
        :class-item: gallery-item training

        .. button-link:: https://www.anyscale.com/blog/training-one-million-machine-learning-models-in-record-time-with-ray

            Training One Million ML Models in Record Time with Ray

    .. grid-item-card:: :bdg-primary:`Blog`
        :class-item: gallery-item training

        .. button-link:: https://www.anyscale.com/blog/many-models-batch-training-at-scale-with-ray-core

            Many Models Batch Training at Scale with Ray Core

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item training

        .. button-ref:: /ray-core/examples/batch_training

            Batch Training with Ray Core

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item data-processing training

        .. button-ref:: /data/examples/batch_training

            Batch Training with Ray Data

    .. grid-item-card:: :bdg-success:`Tutorial`
        :class-item: gallery-item tuning

        .. button-ref:: /tune/tutorials/tune-run

            Tune Basic Parallel Experiments

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item training tuning

        .. button-ref:: /ray-air/examples/batch_tuning

            Batch Training and Tuning using Ray Tune

    .. grid-item-card:: :bdg-warning:`Video`
        :class-item: gallery-item

        .. button-link:: https://www.youtube.com/watch?v=3t26ucTy0Rs

            Scaling Instacart fulfillment ML on Ray


    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-ref:: tune-aim-ref

            Using Aim with Ray Tune For Experiment Management

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-ref:: tune-comet-ref

            Using Comet with Ray Tune For Experiment Management

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tracking monitoring tuning

        .. button-ref:: tune-wandb-ref

            Tracking Your Experiment Process Weights & Biases

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tracking tuning

        .. button-ref:: tune-mlflow-ref

            Using MLflow Tracking & AutoLogging with Tune

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-ref:: /tune/examples/ax_example

            How To Use Tune With Ax

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-ref:: /tune/examples/dragonfly_example

            How To Use Tune With Dragonfly

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-ref:: /tune/examples/skopt_example

            How To Use Tune With Scikit-Optimize

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-ref:: /tune/examples/hyperopt_example

            How To Use Tune With HyperOpt

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-ref:: /tune/examples/bayesopt_example

            How To Use Tune With BayesOpt

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-ref:: /tune/examples/flaml_example

            How To Use Tune With BlendSearch and CFO

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-ref:: /tune/examples/bohb_example

            How To Use Tune With TuneBOHB

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-ref:: /tune/examples/nevergrad_example

            How To Use Tune With Nevergrad

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-ref:: /tune/examples/optuna_example

            How To Use Tune With Optuna

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-ref:: /tune/examples/zoopt_example

            How To Use Tune With ZOOpt

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-ref:: /tune/examples/sigopt_example

            How To Use Tune With SigOpt

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-ref:: /tune/examples/hebo_example

            How To Use Tune With HEBO

    .. grid-item-card:: :bdg-warning:`Video`
        :class-item: gallery-item tuning serving

        .. button-link:: https://www.youtube.com/watch?v=UtH-CMpmxvI

            Productionizing ML at Scale with Ray Serve

    .. grid-item-card:: :bdg-primary:`Blog`
        :class-item: gallery-item tuning serving

        .. button-link:: https://www.anyscale.com/blog/simplify-your-mlops-with-ray-and-ray-serve

            Simplify your MLOps with Ray & Ray Serve

    .. grid-item-card:: :bdg-success:`Tutorial`
        :class-item: gallery-item tuning serving

        .. button-ref:: /serve/getting_started

            Getting Started with Ray Serve

    .. grid-item-card:: :bdg-success:`Tutorial`
        :class-item: gallery-item tuning serving

        .. button-ref:: /serve/model_composition

            Model Composition in Serve

    .. grid-item-card:: :bdg-success:`Tutorial`
        :class-item: gallery-item tuning

        .. button-ref:: /tune/getting-started

            Getting Started with Ray Tune

    .. grid-item-card:: :bdg-primary:`Blog`
        :class-item: gallery-item tuning

        .. button-link:: https://www.anyscale.com/blog/how-to-distribute-hyperparameter-tuning-using-ray-tune

            How to distribute hyperparameter tuning with Ray Tune

    .. grid-item-card:: :bdg-warning:`Video`
        :class-item: gallery-item

        .. button-link:: https://www.youtube.com/watch?v=KgYZtlbFYXE

            Simple Distributed Hyperparameter Optimization

    .. grid-item-card:: :bdg-primary:`Blog`
        :class-item: gallery-item tuning

        .. button-link:: https://www.anyscale.com/blog/hyperparameter-search-hugging-face-transformers-ray-tune

            Hyperparameter Search with 🤗 Transformers

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tensorflow tuning

        .. button-ref:: tune-mnist-keras

            How To Use Tune With Keras & TF Models

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item pytorch tuning

        .. button-ref:: tune-pytorch-cifar-ref

            How To Use Tune With PyTorch Models

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item pytorch tuning

        .. button-ref:: tune-pytorch-lightning-ref

            How To Tune PyTorch Lightning Models

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-ref:: tune-mxnet-example

            How To Tune MXNet Models

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning serving

        .. button-ref:: tune-serve-integration-mnist

            Model Selection & Serving With Ray Serve

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item rl tuning serving

        .. button-ref:: tune-rllib-example

            Tuning RL Experiments With Ray Tune & Ray Serve

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-ref:: tune-xgboost-ref

            A Guide To Tuning XGBoost Parameters With Tune

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-ref:: tune-lightgbm-example

            A Guide To Tuning LightGBM Parameters With Tune

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-ref:: tune-horovod-example

            A Guide To Tuning Horovod Parameters With Tune

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-ref:: tune-huggingface-example

            A Guide To Tuning Huggingface Transformers With Tune

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-link:: https://www.anyscale.com/blog?tag=ray-tune

            More Tune use cases on the Blog

    .. grid-item-card:: :bdg-warning:`Video`
        :class-item: gallery-item pytorch

        .. button-link:: https://www.youtube.com/watch?v=e-A93QftCfc

            Ray Train, PyTorch, TorchX, and distributed deep learning

    .. grid-item-card:: :bdg-primary:`Code example`
        :class-item: gallery-item training

        .. button-link:: https://www.uber.com/blog/elastic-xgboost-ray/

            Elastic Distributed Training with XGBoost on Ray

    .. grid-item-card:: :bdg-success:`Tutorial`
        :class-item: gallery-item

        .. button-ref:: /train/train

            Getting Started with Ray Train

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tuning

        .. button-ref:: /ray-air/examples/huggingface_text_classification

            Fine-tune a 🤗 Transformers model

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item pytorch training

        .. button-ref:: torch_fashion_mnist_ex

            PyTorch Fashion MNIST Training Example

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item pytorch training

        .. button-ref:: train_transformers_example

            Transformers with PyTorch Training Example

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tensorflow training

        .. button-ref:: tensorflow_mnist_example

            TensorFlow MNIST Training Example

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item training

        .. button-ref:: horovod_example

            End-to-end Horovod Training Example

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item pytorch training

        .. button-ref:: lightning_mnist_example

            End-to-end PyTorch Lightning Training Example

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item data-processing

        .. button-ref:: lightning_advanced_example

            Use LightningTrainer with Ray Data and Batch Predictor

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item llm nlp tuning

        .. button-ref:: dolly_lightning_fsdp_finetuning

            Fine-tune LLM with AIR LightningTrainer and FSDP

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tensorflow

        .. button-ref:: tune_train_tf_example

            End-to-end Example for Tuning a TensorFlow Model

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item pytorch tuning

        .. button-ref:: tune_train_torch_example

            End-to-end Example for Tuning a PyTorch Model with PBT

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item training

        .. button-ref:: train_mlflow_example

            Logging Training Runs with MLflow

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tracking

        .. button-ref:: lightning_experiment_tracking

            Using Experiment Tracking Tools in LightningTrainer

    .. grid-item-card:: :bdg-info:`Course`
        :class-item: gallery-item rl

        .. button-link:: https://applied-rl-course.netlify.app/

            Applied Reinforcement Learning with RLlib

    .. grid-item-card:: :bdg-primary:`Blog`
        :class-item: gallery-item rl

        .. button-link:: https://medium.com/distributed-computing-with-ray/intro-to-rllib-example-environments-3a113f532c70

            Intro to RLlib: Example Environments

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item rl tuning

        .. button-link:: https://github.com/ray-project/ray/blob/master/rllib/tuned_examples

            A collection of tuned hyperparameters by RLlib algorithm

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item rl

        .. button-link:: https://github.com/ray-project/rl-experiments

             A collection of reasonably optimized Atari and MuJoCo results for RLlib

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item rl

        .. button-link:: https://medium.com/distributed-computing-with-ray/attention-nets-and-more-with-rllibs-trajectory-view-api-d326339a6e65

            RLlib's trajectory view API and how it enables implementations of GTrXL (attention net) architectures

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item rl

        .. button-link:: https://medium.com/distributed-computing-with-ray/reinforcement-learning-with-rllib-in-the-unity-game-engine-1a98080a7c0d

            A how-to on connecting RLlib with the Unity3D game engine for running visual- and physics-based RL experiments

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item pytorch tensorflow rl

        .. button-link:: https://medium.com/distributed-computing-with-ray/lessons-from-implementing-12-deep-rl-algorithms-in-tf-and-pytorch-1b412009297d

           How we ported 12 of RLlib's algorithms from TensorFlow to PyTorch and what we learnt on the way

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item rl

        .. button-link:: http://bair.berkeley.edu/blog/2018/12/12/rllib

            This blog post is a brief tutorial on multi-agent RL and its design in RLlib

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tensorflow rl

        .. button-link:: https://medium.com/riselab/functional-rl-with-keras-and-tensorflow-eager-7973f81d6345

            Exploration of a functional paradigm for implementing reinforcement learning (RL) algorithms

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item rl

        .. button-link:: https://github.com/ray-project/ray/blob/master/rllib/examples/custom_env.py

            Example of defining and registering a gym env and model for use with RLlib

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item rl

        .. button-link:: https://github.com/ray-project/ray/blob/master/rllib/examples/env_rendering_and_recording.py

            Rendering and recording of an environment

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item rl

        .. button-link:: https://github.com/ray-project/ray/blob/master/rllib/examples/coin_game_env.py

            Coin game example with RLlib

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item rl

        .. button-link:: https://github.com/ray-project/ray/blob/master/rllib/examples/recommender_system_with_recsim_and_slateq.py

            RecSym environment example (for recommender systems) using the SlateQ algorithm

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item rl

        .. button-link:: https://github.com/ray-project/ray/blob/master/rllib/examples/vizdoom_with_attention_net.py

            VizDoom example script using RLlib's auto-attention wrapper

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item rl

        .. button-link:: https://github.com/ray-project/ray/blob/master/rllib/examples/attention_net.py

            Attention Net (GTrXL) learning the "repeat-after-me" environment

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item tensorflow rl

        .. button-link:: https://github.com/ray-project/ray/blob/master/rllib/examples/custom_keras_model.py

            Working with custom Keras models in RLlib

    .. grid-item-card:: :bdg-success:`Tutorial`
        :class-item: gallery-item rl training

        .. button-ref:: /rllib/rllib-training

            Getting Started with RLlib

    .. grid-item-card:: :bdg-warning:`Video`
        :class-item: gallery-item rl

        .. button-link:: https://www.anyscale.com/events/2022/03/29/deep-reinforcement-learning-at-riot-games

            Deep reinforcement learning at Riot Games


    .. grid-item-card:: :bdg-primary:`Blog`
        :class-item: gallery-item

        .. button-link:: https://shopify.engineering/merlin-shopify-machine-learning-platform

            The Magic of Merlin - Shopify's New ML Platform

    .. grid-item-card:: :bdg-success:`Tutorial`
        :class-item: gallery-item training

        .. button-link:: https://drive.google.com/file/d/1BS5lfXfuG5bnI8UM6FdUrR7CiSuWqdLn/view

            Large Scale Deep Learning Training and Tuning with Ray

    .. grid-item-card:: :bdg-primary:`Blog`
        :class-item: gallery-item

        .. button-link:: https://www.instacart.com/company/how-its-made/griffin-how-instacarts-ml-platform-tripled-ml-applications-in-a-year/

            Griffin: How Instacart’s ML Platform Tripled in a year

    .. grid-item-card:: :bdg-warning:`Video`
        :class-item: gallery-item

        .. button-link:: https://www.youtube.com/watch?v=B5v9B5VSI7Q

            Predibase - A low-code deep learning platform built for scale

    .. grid-item-card:: :bdg-primary:`Blog`
        :class-item: gallery-item

        .. button-link:: https://cloud.google.com/blog/products/ai-machine-learning/build-a-ml-platform-with-kubeflow-and-ray-on-gke

            Building a ML Platform with Kubeflow and Ray on GKE

    .. grid-item-card:: :bdg-warning:`Video`
        :class-item: gallery-item

        .. button-link:: https://www.youtube.com/watch?v=_L0lsShbKaY

            Ray Summit Panel - ML Platform on Ray

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item

        .. button-ref:: /ray-air/examples/huggingface_text_classification

            Text classification with Ray

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item pytorch cv

        .. button-ref:: /ray-air/examples/torch_image_example

            Image classification with Ray

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item pytorch cv

        .. button-ref:: /ray-air/examples/torch_detection

            Object detection with Ray

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item

        .. button-ref:: /ray-air/examples/feast_example

            Credit scoring with Ray and Feast

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item data-processing

        .. button-ref:: /ray-air/examples/xgboost_example

            Machine learning on tabular data

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item ts

        .. button-ref:: /ray-core/examples/automl_for_time_series

            AutoML for Time Series with Ray


    .. grid-item-card:: :bdg-primary:`Blog`
        :class-item: gallery-item

        .. button-link:: https://www.anyscale.com/blog/building-highly-available-and-scalable-online-applications-on-ray-at-ant

            Highly Available and Scalable Online Applications on Ray at Ant Group

    .. grid-item-card:: :bdg-primary:`Blog`
        :class-item: gallery-item

        .. button-link:: https://www.anyscale.com/blog/ray-forward-2022

            Ray Forward 2022 Conference: Hyper-scale Ray Application Use Cases

    .. grid-item-card:: :bdg-primary:`Blog`
        :class-item: gallery-item

        .. button-link:: https://www.anyscale.com/blog/ray-breaks-the-usd1-tb-barrier-as-the-worlds-most-cost-efficient-sorting

            A new world record on the CloudSort benchmark using Ray

    .. grid-item-card:: :bdg-secondary:`Code example`
        :class-item: gallery-item

        .. button-ref:: /ray-core/examples/web-crawler

            Speed up your web crawler by parallelizing it with Ray
