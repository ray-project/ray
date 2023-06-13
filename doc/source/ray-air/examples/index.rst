.. _air-examples-ref:

Examples
========


Framework-specific Examples
---------------------------

- :doc:`/ray-air/examples/convert_existing_pytorch_code_to_ray_air`: Get started with Ray AIR from an existing PyTorch codebase
- :doc:`/ray-air/examples/convert_existing_tf_code_to_ray_air`: Get started with Ray AIR from an existing Tensorflow/Keras codebase.
- :doc:`/ray-air/examples/lightgbm_example`: Distributed training with LightGBM
- :doc:`/ray-air/examples/xgboost_example`: Distributed training with XGBoost
- :doc:`/ray-air/examples/analyze_tuning_results`: Distributed tuning with XGBoost
- :doc:`/ray-air/examples/sklearn_example`: Integrating with Scikit-Learn (non-distributed)

Simple Machine Learning
-----------------------
- :doc:`/ray-air/examples/automl_with_ray_air`: Build an AutoML system for time-series forecasting with Ray AIR
- :doc:`/ray-air/examples/batch_tuning`: Perform batch tuning on NYC Taxi Dataset with Ray AIR
- :doc:`/ray-air/examples/batch_forecasting`: Perform batch forecasting on NYC Taxi Dataset with Prophet, ARIMA and Ray AIR

Text/NLP
--------

- :doc:`/ray-air/examples/huggingface_text_classification`: How to use Ray AIR to run Hugging Face Transformers fine-tuning on a text classification task.
- :doc:`/ray-air/examples/gptj_deepspeed_fine_tuning`: How to use Ray AIR to run Hugging Face Transformers with DeepSpeed for fine-tuning a large model.
- :doc:`/ray-air/examples/gptj_batch_prediction`: How to use Ray AIR to do batch prediction with the Hugging Face Transformers GPT-J model.
- :doc:`/ray-air/examples/gptj_serving`: How to use Ray AIR to do online serving with the Hugging Face Transformers GPT-J model.
- :doc:`/ray-air/examples/dreambooth_finetuning`: How to fine-tune a DreamBooth text-to-image model with your own images.
- :doc:`/ray-air/examples/opt_deepspeed_batch_inference`: How to run batch inference on a dataset of texts with a 30B OPT model.
- :doc:`/ray-air/examples/dolly_lightning_fsdp_finetuning`: How to fine-tune a dolly-v2-7b model with Ray AIR LightningTrainer and FSDP.

Image/CV
--------

- :doc:`/ray-air/examples/torch_image_example`
- :doc:`/ray-air/examples/torch_detection`
- :doc:`/ray-air/examples/stablediffusion_batch_prediction`: How to use Ray AIR to do batch prediction with the Stable Diffusion text-to-image model.

Logging & Observability
-----------------------

- :doc:`/ray-air/examples/upload_to_comet_ml`: How to log results and upload models to Comet ML.
- :doc:`/ray-air/examples/upload_to_wandb`: How to log results and upload models to Weights and Biases.

.. _air-rl-examples-ref:

RL (RLlib)
----------

- :doc:`/ray-air/examples/rl_serving_example`
- :doc:`/ray-air/examples/rl_online_example`
- :doc:`/ray-air/examples/rl_offline_example`


Advanced
--------

- :doc:`/ray-air/examples/torch_incremental_learning`: Incrementally train and deploy a PyTorch CV model
- :doc:`/ray-air/examples/feast_example`: Integrate with Feast feature store in both train and inference
