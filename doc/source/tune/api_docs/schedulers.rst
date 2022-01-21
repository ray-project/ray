.. _tune-schedulers:

Trial Schedulers (tune.schedulers)
==================================

In Tune, some hyperparameter optimization algorithms are written as "scheduling algorithms". These Trial Schedulers can early terminate bad trials, pause trials, clone trials, and alter hyperparameters of a running trial.

All Trial Schedulers take in a ``metric``, which is a value returned in the result dict of your Trainable and is maximized or minimized according to ``mode``.

.. code-block:: python

    tune.run( ... , scheduler=Scheduler(metric="accuracy", mode="max"))

.. _schedulers-ref:

Summary
-------

Tune includes distributed implementations of early stopping algorithms such as `Median Stopping Rule <https://research.google.com/pubs/pub46180.html>`__, `HyperBand <https://arxiv.org/abs/1603.06560>`__, and `ASHA <https://openreview.net/forum?id=S1Y7OOlRZ>`__. Tune also includes a distributed implementation of `Population Based Training (PBT) <https://deepmind.com/blog/population-based-training-neural-networks>`__ and `Population Based Bandits (PB2) <https://arxiv.org/abs/2002.02518>`__.

.. tip:: The easiest scheduler to start with is the ``ASHAScheduler`` which will aggressively terminate low-performing trials.

When using schedulers, you may face compatibility issues, as shown in the below compatibility matrix. Certain schedulers cannot be used with Search Algorithms, and certain schedulers require :ref:`checkpointing to be implemented <tune-checkpoint-syncing>`.

Schedulers can dynamically change trial resource requirements during tuning. This is currently implemented in ``ResourceChangingScheduler``, which can wrap around any other scheduler.

.. list-table:: TrialScheduler Feature Compatibility Matrix
   :header-rows: 1

   * - Scheduler
     - Need Checkpointing?
     - SearchAlg Compatible?
     - Example
   * - :ref:`ASHA <tune-scheduler-hyperband>`
     - No
     - Yes
     - :doc:`Link </tune/examples/async_hyperband_example>`
   * - :ref:`Median Stopping Rule <tune-scheduler-msr>`
     - No
     - Yes
     - :ref:`Link <tune-scheduler-msr>`
   * - :ref:`HyperBand <tune-original-hyperband>`
     - Yes
     - Yes
     - :doc:`Link </tune/examples/hyperband_example>`
   * - :ref:`BOHB <tune-scheduler-bohb>`
     - Yes
     - Only TuneBOHB
     - :doc:`Link </tune/examples/bohb_example>`
   * - :ref:`Population Based Training <tune-scheduler-pbt>`
     - Yes
     - Not Compatible
     - :doc:`Link </tune/examples/pbt_function>`
   * - :ref:`Population Based Bandits <tune-scheduler-pb2>`
     - Yes
     - Not Compatible
     - :doc:`Basic Example </tune/examples/pb2_example>`, :doc:`PPO example </tune/examples/pb2_ppo_example>`

.. _tune-scheduler-hyperband:

ASHA (tune.schedulers.ASHAScheduler)
------------------------------------

The `ASHA <https://openreview.net/forum?id=S1Y7OOlRZ>`__ scheduler can be used by setting the ``scheduler`` parameter of ``tune.run``, e.g.

.. code-block:: python

    asha_scheduler = ASHAScheduler(
        time_attr='training_iteration',
        metric='episode_reward_mean',
        mode='max',
        max_t=100,
        grace_period=10,
        reduction_factor=3,
        brackets=1)
    tune.run( ... , scheduler=asha_scheduler)

Compared to the original version of HyperBand, this implementation provides better parallelism and avoids straggler issues during eliminations. **We recommend using this over the standard HyperBand scheduler.** An example of this can be found here: :doc:`/tune/examples/async_hyperband_example`.

Even though the original paper mentions a bracket count of 3, discussions with the authors concluded that the value should be left to 1 bracket. This is the default used if no value is provided for the ``brackets`` argument.

.. autoclass:: ray.tune.schedulers.AsyncHyperBandScheduler

.. autoclass:: ray.tune.schedulers.ASHAScheduler

.. _tune-original-hyperband:

HyperBand (tune.schedulers.HyperBandScheduler)
----------------------------------------------

Tune implements the `standard version of HyperBand <https://arxiv.org/abs/1603.06560>`__. **We recommend using the ASHA Scheduler over the standard HyperBand scheduler.**

.. autoclass:: ray.tune.schedulers.HyperBandScheduler


HyperBand Implementation Details
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Implementation details may deviate slightly from theory but are focused on increasing usability. Note: ``R``, ``s_max``, and ``eta`` are parameters of HyperBand given by the paper. See `this post <https://homes.cs.washington.edu/~jamieson/hyperband.html>`_ for context.

1. Both ``s_max`` (representing the ``number of brackets - 1``) and ``eta``, representing the downsampling rate, are fixed.  In many practical settings, ``R``, which represents some resource unit and often the number of training iterations, can be set reasonably large, like ``R >= 200``. For simplicity, assume ``eta = 3``. Varying ``R`` between ``R = 200`` and ``R = 1000`` creates a huge range of the number of trials needed to fill up all brackets.

.. image:: /images/hyperband_bracket.png

On the other hand, holding ``R`` constant at ``R = 300`` and varying ``eta`` also leads to HyperBand configurations that are not very intuitive:

.. image:: /images/hyperband_eta.png

The implementation takes the same configuration as the example given in the paper and exposes ``max_t``, which is not a parameter in the paper.

2. The example in the `post <https://homes.cs.washington.edu/~jamieson/hyperband.html>`_ to calculate ``n_0`` is actually a little different than the algorithm given in the paper. In this implementation, we implement ``n_0`` according to the paper (which is `n` in the below example):

.. image:: /images/hyperband_allocation.png


3. There are also implementation specific details like how trials are placed into brackets which are not covered in the paper. This implementation places trials within brackets according to smaller bracket first - meaning that with low number of trials, there will be less early stopping.

.. _tune-scheduler-msr:

Median Stopping Rule (tune.schedulers.MedianStoppingRule)
---------------------------------------------------------

The Median Stopping Rule implements the simple strategy of stopping a trial if its performance falls below the median of other trials at similar points in time.

.. autoclass:: ray.tune.schedulers.MedianStoppingRule

.. _tune-scheduler-pbt:

Population Based Training (tune.schedulers.PopulationBasedTraining)
-------------------------------------------------------------------

Tune includes a distributed implementation of `Population Based Training (PBT) <https://deepmind.com/blog/population-based-training-neural-networks>`__. This can be enabled by setting the ``scheduler`` parameter of ``tune.run``, e.g.

.. code-block:: python

    pbt_scheduler = PopulationBasedTraining(
            time_attr='time_total_s',
            metric='mean_accuracy',
            mode='max',
            perturbation_interval=600.0,
            hyperparam_mutations={
                "lr": [1e-3, 5e-4, 1e-4, 5e-5, 1e-5],
                "alpha": lambda: random.uniform(0.0, 1.0),
            ...
            })
    tune.run( ... , scheduler=pbt_scheduler)

When the PBT scheduler is enabled, each trial variant is treated as a member of the population. Periodically, top-performing trials are checkpointed (this requires your Trainable to support :ref:`save and restore <tune-checkpoint-syncing>`). Low-performing trials clone the checkpoints of top performers and perturb the configurations in the hope of discovering an even better variation.

You can run this :doc:`toy PBT example </tune/examples/pbt_function>` to get an idea of how how PBT operates. When training in PBT mode, a single trial may see many different hyperparameters over its lifetime, which is recorded in its ``result.json`` file. The following figure generated by the example shows PBT with optimizing a LR schedule over the course of a single experiment:

.. image:: ../images/pbt.png

.. autoclass:: ray.tune.schedulers.PopulationBasedTraining


.. _tune-scheduler-pbt-replay:

Population Based Training Replay (tune.schedulers.PopulationBasedTrainingReplay)
--------------------------------------------------------------------------------

Tune includes a utility to replay hyperparameter schedules of Population Based Training runs.
You just specify an existing experiment directory and the ID of the trial you would
like to replay. The scheduler accepts only one trial, and it will update its
config according to the obtained schedule.

.. code-block:: python

    replay = PopulationBasedTrainingReplay(
        experiment_dir="~/ray_results/pbt_experiment/",
        trial_id="XXXXX_00001")
    tune.run(
        ...,
        scheduler=replay)

See :ref:`here for an example <tune-advanced-tutorial-pbt-replay>` on how to use the
replay utility in practice.

.. autoclass:: ray.tune.schedulers.PopulationBasedTrainingReplay


.. _tune-scheduler-pb2:

Population Based Bandits (PB2) (tune.schedulers.pb2.PB2)
--------------------------------------------------------

Tune includes a distributed implementation of `Population Based Bandits (PB2) <https://arxiv.org/abs/2002.02518>`__. This algorithm builds upon PBT, with the main difference being that instead of using random perturbations, PB2 selects new hyperparameter configurations using a Gaussian Process model.

The Tune implementation of PB2 requires GPy and sklearn to be installed:

.. code-block:: bash

    pip install GPy sklearn


PB2 can be enabled by setting the ``scheduler`` parameter of ``tune.run``, e.g.:

.. code-block:: python

    from ray.tune.schedulers.pb2 import PB2

    pb2_scheduler = PB2(
            time_attr='time_total_s',
            metric='mean_accuracy',
            mode='max',
            perturbation_interval=600.0,
            hyperparam_bounds={
                "lr": [1e-3, 1e-5],
                "alpha": [0.0, 1.0],
            ...
            })
    tune.run( ... , scheduler=pb2_scheduler)


When the PB2 scheduler is enabled, each trial variant is treated as a member of the population. Periodically, top-performing trials are checkpointed (this requires your Trainable to support :ref:`save and restore <tune-checkpoint-syncing>`). Low-performing trials clone the checkpoints of top performers and perturb the configurations in the hope of discovering an even better variation.

The primary motivation for PB2 is the ability to find promising hyperparamters with only a small population size. With that in mind, you can run this :doc:`PB2 PPO example </tune/examples/pb2_ppo_example>` to compare PB2 vs. PBT, with a population size of ``4`` (as in the paper). The example uses the ``BipedalWalker`` environment so does not require any additional licenses.

.. autoclass:: ray.tune.schedulers.pb2.PB2


.. _tune-scheduler-bohb:

BOHB (tune.schedulers.HyperBandForBOHB)
---------------------------------------

This class is a variant of HyperBand that enables the `BOHB Algorithm <https://arxiv.org/abs/1807.01774>`_. This implementation is true to the original HyperBand implementation and does not implement pipelining nor straggler mitigation.

This is to be used in conjunction with the Tune BOHB search algorithm. See :ref:`TuneBOHB <suggest-TuneBOHB>` for package requirements, examples, and details.

An example of this in use can be found here: :doc:`/tune/examples/bohb_example`.

.. autoclass:: ray.tune.schedulers.HyperBandForBOHB

ResourceChangingScheduler
-------------------------

This class is a utility scheduler, allowing for trial resource requirements to be changed during tuning. It wraps around another scheduler and uses its decisions.

* If you are using the Trainable (class) API for tuning, your Trainable must implement ``Trainable.update_resources``, which will let your model know about the new resources assigned. You can also obtain the current trial resources by calling ``Trainable.trial_resources``.

* If you are using the functional API for tuning, the current trial resources can be obtained by calling `tune.get_trial_resources()` inside the training function. The function should be able to :ref:`load and save checkpoints <tune-checkpoint-syncing>` (the latter preferably every iteration).

An example of this in use can be found here: :doc:`/tune/examples/xgboost_dynamic_resources_example`.

.. autoclass:: ray.tune.schedulers.ResourceChangingScheduler

evenly_distribute_cpus_gpus
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.tune.schedulers.resource_changing_scheduler.evenly_distribute_cpus_gpus

evenly_distribute_cpus_gpus_distributed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.tune.schedulers.resource_changing_scheduler.evenly_distribute_cpus_gpus_distributed

FIFOScheduler
-------------

.. autoclass:: ray.tune.schedulers.FIFOScheduler

TrialScheduler
--------------

.. autoclass:: ray.tune.schedulers.TrialScheduler
    :members:

Shim Instantiation (tune.create_scheduler)
------------------------------------------
There is also a shim function that constructs the scheduler based on the provided string. This can be useful if the scheduler you want to use changes often (e.g., specifying the scheduler via a CLI option or config file).

.. automethod:: ray.tune.create_scheduler
