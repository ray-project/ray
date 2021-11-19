========
Examples
========

.. Keep this in sync with ray/python/ray/tune/examples/README.rst

If any example is broken, or if you'd like to add an example to this page, feel free to raise an issue on our Github repository.

.. tip:: Check out :ref:`the Tune tutorials page <tune-guides>` for guides on how to use Tune with your preferred machine learning library.

.. _tune-general-examples:

General Examples
----------------

- :doc:`/tune/examples/tune_basic_example`: Simple example for doing a basic random and grid search.
- :doc:`/tune/examples/async_hyperband_example`: Example of using a simple tuning function with AsyncHyperBandScheduler.
- :doc:`/tune/examples/hyperband_function_example`: Example of using a Trainable function with HyperBandScheduler.  Also uses the AsyncHyperBandScheduler.
- :doc:`/tune/examples/pbt_function`: Example of using the function API with a PopulationBasedTraining scheduler.
- :doc:`/tune/examples/pb2_example`: Example of using the Population-based Bandits (PB2) scheduler.
- :doc:`/tune/examples/logging_example`: Example of custom loggers and custom trial directory naming.

**Trainable Class Examples**

Though it is preferable to use the Function API, Tune also supports a Class-based API for training.

- :doc:`/tune/examples/hyperband_example`: Example of using a Trainable class with HyperBandScheduler. Also uses the AsyncHyperBandScheduler.
- :doc:`/tune/examples/pbt_example`: Example of using a Trainable class with PopulationBasedTraining scheduler.

.. - :doc:`/tune/examples/durable_trainable_example`: Example using a durable storage mechanism in the Trainable.


Search Algorithm Examples
-------------------------

- :doc:`/tune/examples/ax_example`: Example script showing usage of :ref:`AxSearch <tune-ax>` [`Ax website <https://ax.dev/>`__]
- :doc:`/tune/examples/dragonfly_example`: Example script showing usage of :ref:`DragonflySearch <Dragonfly>` [`Dragonfly website <https://dragonfly-opt.readthedocs.io/>`__]
- :doc:`/tune/examples/skopt_example`: Example script showing usage of :ref:`SkoptSearch <skopt>` [`Scikit-Optimize website <https://scikit-optimize.github.io>`__]
- :doc:`/tune/examples/hyperopt_example`: Example script showing usage of :ref:`HyperOptSearch <tune-hyperopt>` [`HyperOpt website <http://hyperopt.github.io/hyperopt>`__]
- :doc:`/tune/examples/hyperopt_conditional_search_space_example`: Example script showing usage of :ref:`HyperOptSearch <tune-hyperopt>` [`HyperOpt website <http://hyperopt.github.io/hyperopt>`__] with a conditional search space
- :doc:`/tune/examples/bayesopt_example`: Example script showing usage of :ref:`BayesOptSearch <bayesopt>` [`BayesianOptimization website <https://github.com/fmfn/BayesianOptimization>`__]
- :doc:`/tune/examples/blendsearch_example`: Example script showing usage of :ref:`BlendSearch <BlendSearch>` [`BlendSearch website <https://github.com/microsoft/FLAML/tree/main/flaml/tune>`__]
- :doc:`/tune/examples/cfo_example`: Example script showing usage of :ref:`CFO <CFO>` [`CFO website <https://github.com/microsoft/FLAML/tree/main/flaml/tune>`__]
- :doc:`/tune/examples/bohb_example`: Example script showing usage of :ref:`TuneBOHB <suggest-TuneBOHB>` [`BOHB website <https://github.com/automl/HpBandSter>`__]
- :doc:`/tune/examples/nevergrad_example`: Example script showing usage of :ref:`NevergradSearch <nevergrad>` [`Nevergrad website <https://github.com/facebookresearch/nevergrad>`__]
- :doc:`/tune/examples/optuna_example`: Example script showing usage of :ref:`OptunaSearch <tune-optuna>` [`Optuna website <https://optuna.org/>`__]
- :doc:`/tune/examples/optuna_define_by_run_example`: Example script showing usage of :ref:`OptunaSearch <tune-optuna>` [`Optuna website <https://optuna.org/>`__] with a define-by-run function
- :doc:`/tune/examples/optuna_multiobjective_example`: Example script showing usage of :ref:`OptunaSearch <tune-optuna>` [`Optuna website <https://optuna.org/>`__] for multi-objective optimization
- :doc:`/tune/examples/zoopt_example`: Example script showing usage of :ref:`ZOOptSearch <zoopt>` [`ZOOpt website <https://github.com/polixir/ZOOpt>`__]
- :doc:`/tune/examples/sigopt_example`: Example script showing usage of :ref:`SigOptSearch <sigopt>` [`SigOpt website <https://sigopt.com/>`__]
- :doc:`/tune/examples/hebo_example`: Example script showing usage of :ref:`HEBOSearch <tune-hebo>` [`HEBO website <https://github.com/huawei-noah/HEBO/tree/master/HEBO>`__]


**Sigopt (Contributed)**

- :doc:`/tune/examples/sigopt_multi_objective_example`: Example using Sigopt's multi-objective functionality.
- :doc:`/tune/examples/sigopt_prior_beliefs_example`: Example using Sigopt's support for prior beliefs.


tune-sklearn examples
---------------------

See the `ray-project/tune-sklearn examples <https://github.com/ray-project/tune-sklearn/tree/master/examples>`__ for a comprehensive list of examples leveraging Tune's sklearn interface.

- `tune-sklearn with xgboost <https://github.com/ray-project/tune-sklearn/blob/master/examples/xgbclassifier.py>`__
- `tune-sklearn with sklearn pipelines <https://github.com/ray-project/tune-sklearn/blob/master/examples/sklearn_pipeline.py>`__
- `tune-sklearn with Bayesian Optimization <https://github.com/ray-project/tune-sklearn/blob/master/examples/hyperopt_sgd.py>`__


Framework-specific Examples
---------------------------

PyTorch
~~~~~~~

- :doc:`/tune/examples/mnist_pytorch`: Converts the PyTorch MNIST example to use Tune with the function-based API. Also shows how to easily convert something relying on argparse to use Tune.
- :doc:`/tune/examples/ddp_mnist_torch`: An example showing how to use DistributedDataParallel with Ray Tune. This enables both distributed training and distributed hyperparameter tuning.
- :doc:`/tune/examples/cifar10_pytorch`: Uses Pytorch to tune a simple model on CIFAR10.
- :doc:`/tune/examples/pbt_convnet_function_example`: Example training a ConvNet with checkpointing in function API.

.. - :doc:`/tune/examples/pbt_convnet_example`: Example of training a Memory NN on bAbI with Keras using PBT.
.. - :doc:`/tune/examples/mnist_pytorch_trainable`: Converts the PyTorch MNIST example to use Tune with Trainable API. Also uses the HyperBandScheduler and checkpoints the model at the end.

Pytorch Lightning
~~~~~~~~~~~~~~~~~

- :doc:`/tune/examples/mnist_ptl_mini`: A minimal example of using `Pytorch Lightning <https://github.com/PyTorchLightning/pytorch-lightning>`_ to train a MNIST model. This example utilizes the Ray Tune-provided :ref:`PyTorch Lightning callbacks <tune-integration-pytorch-lightning>`. See also :ref:`this tutorial for a full walkthrough <tune-pytorch-lightning>`.
- :doc:`/tune/examples/mnist_pytorch_lightning`: A comprehensive example using `Pytorch Lightning <https://github.com/PyTorchLightning/pytorch-lightning>`_ to train a MNIST model. This example showcases how to use various search optimization techniques. It utilizes the Ray Tune-provided :ref:`PyTorch Lightning callbacks <tune-integration-pytorch-lightning>`.
- :ref:`A walkthrough tutorial for using Ray Tune with Pytorch-Lightning <tune-pytorch-lightning>`.

Wandb, MLflow
~~~~~~~~~~~~~

- :ref:`Tutorial <tune-wandb>` for using `wandb <https://www.wandb.ai/>`__ with Ray Tune
- :doc:`/tune/examples/wandb_example`: Example for using `Weights and Biases <https://www.wandb.ai/>`__ with Ray Tune.
- :doc:`/tune/examples/mlflow_example`: Example for using `MLflow <https://github.com/mlflow/mlflow/>`__ with Ray Tune.
- :doc:`/tune/examples/mlflow_ptl_example`: Example for using `MLflow <https://github.com/mlflow/mlflow/>`__ and `Pytorch Lightning <https://github.com/PyTorchLightning/pytorch-lightning>`_ with Ray Tune.

Tensorflow/Keras
~~~~~~~~~~~~~~~~

- :doc:`/tune/examples/tune_mnist_keras`: Converts the Keras MNIST example to use Tune with the function-based API and a Keras callback. Also shows how to easily convert something relying on argparse to use Tune.
- :doc:`/tune/examples/pbt_memnn_example`: Example of training a Memory NN on bAbI with Keras using PBT.
- :doc:`/tune/examples/tf_mnist_example`: Converts the Advanced TF2.0 MNIST example to use Tune with the Trainable. This uses `tf.function`. Original code from tensorflow: https://www.tensorflow.org/tutorials/quickstart/advanced

MXNet
~~~~~

- :doc:`/tune/examples/mxnet_example`: Simple example for using MXNet with Tune.
- :doc:`/tune/examples/tune_cifar10_gluon`: MXNet Gluon example to use Tune with the function-based API on CIFAR-10 dataset.


Horovod
~~~~~~~

- :doc:`/tune/examples/horovod_simple`: Leverages the :ref:`Horovod-Tune <tune-integration-horovod>` integration to launch a distributed training + tuning job.

XGBoost, LightGBM
~~~~~~~~~~~~~~~~~

- :ref:`XGBoost tutorial <tune-xgboost>`: A guide to tuning XGBoost parameters with Tune.
- :doc:`/tune/examples/xgboost_example`: Trains a basic XGBoost model with Tune with the function-based API and an XGBoost callback.
- :doc:`/tune/examples/xgboost_dynamic_resources_example`: Trains a basic XGBoost model with Tune with the class-based API and a ResourceChangingScheduler, ensuring all resources are being used at all time.
- :doc:`/tune/examples/lightgbm_example`: Trains a basic LightGBM model with Tune with the function-based API and a LightGBM callback.

RLlib
~~~~~

- :doc:`/tune/examples/pbt_ppo_example`: Example of optimizing a distributed RLlib algorithm (PPO) with the PopulationBasedTraining scheduler.
- :doc:`/tune/examples/pb2_ppo_example`: Example of optimizing a distributed RLlib algorithm (PPO) with the PB2 scheduler. Uses a small population size of 4, so can train on a laptop.


|:hugging_face:| Huggingface Transformers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- :doc:`/tune/examples/pbt_transformers`: Fine-tunes a Huggingface transformer with Tune Population Based Training.


Contributed Examples
--------------------

- :doc:`/tune/examples/pbt_tune_cifar10_with_keras`: A contributed example of tuning a Keras model on CIFAR10 with the PopulationBasedTraining scheduler.
- :doc:`/tune/examples/genetic_example`: Optimizing the michalewicz function using the contributed GeneticSearch algorithm with AsyncHyperBandScheduler.


Open Source Projects using Tune
-------------------------------

Here are some of the popular open source repositories and research projects that leverage Tune. Feel free to submit a pull-request adding (or requesting a removal!) of a listed project.

- `Softlearning <https://github.com/rail-berkeley/softlearning>`_: Softlearning is a reinforcement learning framework for training maximum entropy policies in continuous domains. Includes the official implementation of the Soft Actor-Critic algorithm.
- `Flambe <https://github.com/asappresearch/flambe>`_: An ML framework to accelerate research and its path to production. See `flambe.ai <https://flambe.ai>`_.
- `Population Based Augmentation <https://github.com/arcelien/pba>`_: Population Based Augmentation (PBA) is a algorithm that quickly and efficiently learns data augmentation functions for neural network training. PBA matches state-of-the-art results on CIFAR with one thousand times less compute.
- `Fast AutoAugment by Kakao <https://github.com/kakaobrain/fast-autoaugment>`_: Fast AutoAugment (Accepted at NeurIPS 2019) learns augmentation policies using a more efficient search strategy based on density matching.
- `Allentune <https://github.com/allenai/allentune>`_: Hyperparameter Search for AllenNLP from AllenAI.
- `machinable <https://github.com/frthjf/machinable>`_: A modular configuration system for machine learning research. See `machinable.org <https://machinable.org>`_.
- `NeuroCard <https://github.com/neurocard/neurocard>`_: NeuroCard (Accepted at VLDB 2021) is a neural cardinality estimator for multi-table join queries. It uses state of the art deep density models to learn correlations across relational database tables.
