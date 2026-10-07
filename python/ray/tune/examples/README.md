# Tune Examples

<!-- Keep this in sync with ray/doc/tune-examples.rst -->

In our repository, we provide a variety of examples for the various use cases and features of Tune.

If any example is broken, or if you'd like to add an example to this page, feel free to raise an issue on our Github repository.


## General Examples

- [async_hyperband_example](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/async_hyperband_example.py): Example of using a Trainable class with AsyncHyperBandScheduler.
- [hyperband_example](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperband_example.py): Example of using a Trainable class with HyperBandScheduler. Also uses the Experiment class API for specifying the experiment configuration. Also uses the AsyncHyperBandScheduler.
- [pbt_example](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/pbt_example.py): Example of using a Trainable class with PopulationBasedTraining scheduler.
- [PBT with Function API](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/pbt_function.py): Example of using the function API with a PopulationBasedTraining scheduler.
- [pbt_ppo_example](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/pbt_ppo_example.py): Example of optimizing a distributed RLlib algorithm (PPO) with the PopulationBasedTraining scheduler.
- [logging_example](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/logging_example.py): Example of custom loggers and custom trial directory naming.
- [custom_func_checkpointing](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/logging_example.py): Example of custom checkpointing logic using the function API.

## Search Algorithm Examples

- [Ax example](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/ax_example.py): Optimize a Hartmann function with [Ax](https://ax.dev) with 4 parallel workers.
- [Nevergrad example](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/nevergrad_example.py): Optimize a simple toy function with the gradient-free optimization package [Nevergrad](https://github.com/facebookresearch/nevergrad) with 4 parallel workers.
- [Bayesian Optimization example](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/bayesopt_example.py): Optimize a simple toy function using [Bayesian Optimization](https://github.com/fmfn/BayesianOptimization) with 4 parallel workers.

## Tensorflow/Keras Examples

- [tune_mnist_keras](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/tune_mnist_keras.py): Converts the Keras MNIST example to use Tune with the function-based API and a Keras callback. Also shows how to easily convert something relying on argparse to use Tune.
- [pbt_memnn_example](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/pbt_memnn_example.py): Example of training a Memory NN on bAbI with Keras using PBT.
- [Tensorflow 2 Example](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/tf_mnist_example.py): Converts the Advanced TF2.0 MNIST example to use Tune with the Trainable. This uses `tf.function`. Original code from tensorflow: https://www.tensorflow.org/tutorials/quickstart/advanced


## PyTorch Examples

- [mnist_pytorch](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/mnist_pytorch.py): Converts the PyTorch MNIST example to use Tune with the function-based API. Also shows how to easily convert something relying on argparse to use Tune.
- [mnist_pytorch_trainable](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/mnist_pytorch_trainable.py): Converts the PyTorch MNIST example to use Tune with Trainable API. Also uses the HyperBandScheduler and checkpoints the model at the end.


## PyTorch Lightning Examples

For a full walkthrough of tuning a PyTorch Lightning model with Ray Tune, see the
[Using PyTorch Lightning with Tune](https://docs.ray.io/en/latest/tune/examples/tune-pytorch-lightning.html) tutorial.

- [mnist_ptl_mini](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/mnist_ptl_mini.py): A minimal example of tuning a PyTorch Lightning MNIST classifier with Ray Tune.
- [mlflow_ptl](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/mlflow_ptl.py): Example for using [MLflow](https://github.com/mlflow/mlflow/) and PyTorch Lightning with Ray Tune.


## XGBoost Example

- [xgboost_example](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/xgboost_example.py): Trains a basic XGBoost model with Tune with the function-based API and a XGBoost callback.


## XGBoost with Dynamic Resources Example

- [xgboost_dynamic_resources_example](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/xgboost_dynamic_resources_example.py): Trains a basic XGBoost model with Tune with the class-based API and a ResourceChangingScheduler, ensuring all resources are being used at all time.


## LightGBM Example

- [lightgbm_example](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/lightgbm_example.py): Trains a basic LightGBM model with Tune with the function-based API and a LightGBM callback.

## Huggingface Transformers Example

- [pbt_transformers](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/pbt_transformers/pbt_transformers.py): Fine-tunes a Huggingface transformer with Tune Population Based Training.


## Contributed Examples

- [pbt_tune_cifar10_with_keras](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/pbt_tune_cifar10_with_keras.py): A contributed example of tuning a Keras model on CIFAR10 with the PopulationBasedTraining scheduler.
- [hyperopt_conditional_search_space_example](https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperopt_conditional_search_space_example.py): Conditional search space example using HyperOpt.
