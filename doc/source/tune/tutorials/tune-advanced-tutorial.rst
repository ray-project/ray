A Guide to Population Based Training
====================================

Tune includes a distributed implementation of `Population Based Training (PBT) <https://deepmind.com/blog/population-based-training-neural-networks>`__ as
a :ref:`scheduler <tune-scheduler-pbt>`.

.. image:: /images/tune_advanced_paper1.png


PBT starts by training many neural networks in parallel with random hyperparameters, using information from the rest of the population to refine these
hyperparameters and allocate resources to promising models. Let's walk through how to use this algorithm.

.. contents::
    :local:
    :backlinks: none

Function API with Population Based Training
-------------------------------------------

PBT takes its inspiration from genetic algorithms where each member of the population
can exploit information from the remainder of the population. For example, a worker might
copy the model parameters from a better performing worker. It can also explore new hyperparameters by
changing the current values randomly.

As the training of the population of neural networks progresses, this process of exploiting and exploring
is performed periodically, ensuring that all the workers in the population have a good base level of performance
and also that new hyperparameters are consistently explored.

This means that PBT can quickly exploit good hyperparameters, can dedicate more training time to
promising models and, crucially, can adapt the hyperparameter values throughout training,
leading to automatic learning of the best configurations.

First we define a training function that trains a ConvNet model using SGD.

.. literalinclude:: /../../python/ray/tune/examples/pbt_convnet_function_example.py
    :language: python
    :start-after: __train_begin__
    :end-before: __train_end__

The example reuses some of the functions in ray/tune/examples/mnist_pytorch.py, and is also a good
demo for how to decouple the tuning logic and original training code.

Here, we also need to take in a ``checkpoint_dir`` arg since checkpointing is required for the exploitation process in PBT.
We have to both load in the checkpoint if one is provided, and periodically save our
model state in a checkpoint- in this case every 5 iterations. With SGD, there's no need to checkpoint the optimizer
since it does not depend on previous states, but this is necessary with other optimizers like Adam.

Then, we define a PBT scheduler:

.. literalinclude:: /../../python/ray/tune/examples/pbt_convnet_function_example.py
   :language: python
   :start-after: __pbt_begin__
   :end-before: __pbt_end__

Some of the most important parameters are:

- ``hyperparam_mutations`` and ``custom_explore_fn`` are used to mutate the hyperparameters.
  ``hyperparam_mutations`` is a dictionary where each key/value pair specifies the candidates
  or function for a hyperparameter. custom_explore_fn is applied after built-in perturbations
  from hyperparam_mutations are applied, and should return config updated as needed.

- ``resample_probability``: The probability of resampling from the original distribution
  when applying hyperparam_mutations. If not resampled, the value will be perturbed by a
  factor of 1.2 or 0.8 if continuous, or changed to an adjacent value if discrete. Note that
  ``resample_probability`` by default is 0.25, thus hyperparameter with a distribution
  may go out of the specific range.

Now we can kick off the tuning process by invoking tune.run:

.. literalinclude:: /../../python/ray/tune/examples/pbt_convnet_function_example.py
   :language: python
   :start-after: __tune_begin__
   :end-before: __tune_end__

During the training, we can constantly check the status of the models from console log:

.. code-block:: bash

    == Status ==
    Memory usage on this node: 11.2/16.0 GiB
    PopulationBasedTraining: 12 checkpoints, 5 perturbs
    Resources requested: 0/16 CPUs, 0/0 GPUs, 0.0/4.83 GiB heap, 0.0/1.66 GiB objects
    Result logdir: /Users/foo/ray_results/pbt_test
    Number of trials: 4 (4 TERMINATED)
    +---------------------------+------------+-------+-----------+------------+----------+--------+------------------+
    | Trial name                | status     | loc   |        lr |   momentum |      acc |   iter |   total time (s) |
    |---------------------------+------------+-------+-----------+------------+----------+--------+------------------|
    | train_convnet_b2732_00000 | TERMINATED |       | 0.221776  |   0.608416 | 0.95625  |     59 |          13.0862 |
    | train_convnet_b2732_00001 | TERMINATED |       | 0.0734679 |   0.1484   | 0.934375 |     59 |          13.1084 |
    | train_convnet_b2732_00002 | TERMINATED |       | 0.0376862 |   0.8      | 0.971875 |     46 |          10.2909 |
    | train_convnet_b2732_00003 | TERMINATED |       | 0.0471078 |   0.8      | 0.95     |     51 |          11.3355 |
    +---------------------------+------------+-------+-----------+------------+----------+--------+------------------+

In {LOG_DIR}/{MY_EXPERIMENT_NAME}/, all mutations are logged in pbt_global.txt
and individual policy perturbations are recorded in pbt_policy_{i}.txt. Tune logs:
[target trial tag, clone trial tag, target trial iteration, clone trial iteration,
old config, new config] on each perturbation step.

Checking the accuracy:

.. code-block:: python

    # Plot by wall-clock time
    dfs = analysis.fetch_trial_dataframes()
    # This plots everything on the same plot
    ax = None
    for d in dfs.values():
        ax = d.plot("training_iteration", "mean_accuracy", ax=ax, legend=False)

    plt.xlabel("iterations")
    plt.ylabel("Test Accuracy")

    print('best config:', analysis.get_best_config("mean_accuracy"))

.. image:: /images/tune_advanced_plot1.png

.. _tune-advanced-tutorial-pbt-replay:

Replaying a PBT run
-------------------
A run of Population Based Training ends with fully trained models. However, sometimes
you might like to train the model from scratch, but use the same hyperparameter
schedule as obtained from PBT. Ray Tune offers a replay utility for this.

All you need to do is pass the policy log file for the trial you want to replay.
This is usually stored in the experiment directory, for instance
``~/ray_results/pbt_test/pbt_policy_ba982_00000.txt``.

The replay utility reads the original configuration for the trial and updates it
each time when it was originally perturbed. You can (and should)
thus just use the same ``Trainable`` for the replay run.

.. code-block:: python

    from ray import tune

    from ray.tune.examples.pbt_convnet_example import PytorchTrainable
    from ray.tune.schedulers import PopulationBasedTrainingReplay

    replay = PopulationBasedTrainingReplay(
        "~/ray_results/pbt_test/pbt_policy_ba982_00003.txt")

    tune.run(
        PytorchTrainable,
        scheduler=replay,
        stop={"training_iteration": 100})

DCGAN with PBT
--------------

The Generative Adversarial Networks (GAN) (Goodfellow et al., 2014) framework learns generative
models via a training paradigm consisting of two competing modules – a generator and a
discriminator. GAN training can be remarkably brittle and unstable in the face of suboptimal
hyperparameter selection with generators often collapsing to a single mode or diverging entirely.

As presented in `Population Based Training (PBT) <https://deepmind.com/blog/population-based-training-neural-networks>`__,
PBT can help with the DCGAN training. We will now walk through how to do this in Tune.
Complete code example at `github <https://github.com/ray-project/ray/tree/master/python/ray/tune/examples/pbt_dcgan_mnist>`__

We define the Generator and Discriminator with standard Pytorch API:

.. literalinclude:: /../../python/ray/tune/examples/pbt_dcgan_mnist/common.py
   :language: python
   :start-after: __GANmodel_begin__
   :end-before: __GANmodel_end__

To train the model with PBT, we need to define a metric for the scheduler to evaluate
the model candidates. For a GAN network, inception score is arguably the most
commonly used metric. We trained a mnist classification model (LeNet) and use
it to inference the generated images and evaluate the image quality.

.. literalinclude:: /../../python/ray/tune/examples/pbt_dcgan_mnist/common.py
   :language: python
   :start-after: __INCEPTION_SCORE_begin__
   :end-before: __INCEPTION_SCORE_end__

We define a training function that includes a Generator and a Discriminator, each with an
independent learning rate and optimizer. We make sure to implement checkpointing for our training.

.. literalinclude:: /../../python/ray/tune/examples/pbt_dcgan_mnist/pbt_dcgan_mnist_func.py
   :language: python
   :start-after: __Train_begin__
   :end-before: __Train_end__

We specify inception score as the metric and start the tuning:

.. literalinclude:: /../../python/ray/tune/examples/pbt_dcgan_mnist/pbt_dcgan_mnist_func.py
   :language: python
   :start-after: __tune_begin__
   :end-before: __tune_end__

The trained Generator models can be loaded from log directory, and generate images
from noise signals.

Visualization
~~~~~~~~~~~~~

Below, we visualize the increasing inception score from the training logs.

.. code-block:: python

    lossG = [df['is_score'].tolist() for df in list(analysis.trial_dataframes.values())]

    plt.figure(figsize=(10,5))
    plt.title("Inception Score During Training")
    for i, lossg in enumerate(lossG):
        plt.plot(lossg,label=i)

    plt.xlabel("iterations")
    plt.ylabel("is_score")
    plt.legend()
    plt.show()

.. image:: /images/tune_advanced_dcgan_inscore.png

And the Generator loss:

.. code-block:: python

    lossG = [df['lossg'].tolist() for df in list(analysis.trial_dataframes.values())]

    plt.figure(figsize=(10,5))
    plt.title("Generator Loss During Training")
    for i, lossg in enumerate(lossG):
        plt.plot(lossg,label=i)

    plt.xlabel("iterations")
    plt.ylabel("LossG")
    plt.legend()
    plt.show()

.. image:: /images/tune_advanced_dcgan_Gloss.png

Training of the MNist Generator takes a couple of minutes. The example can be easily
altered to generate images for other datasets, e.g. cifar10 or LSUN.
