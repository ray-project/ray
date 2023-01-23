.. include:: /_includes/rllib/announcement.rst

.. include:: /_includes/rllib/we_are_hiring.rst

Fault Tolerance And Elastic Training
====================================

RLlib handles common failures modes, such as machine failures, spot instance preemption,
network outages, or Ray cluster failures.

There are three main areas:

* Worker recovery
* Experiment level fault tolerance with Ray Tune
* Environment fault tolerance.


Elastic Workers
---------------

RLlib supports self-recovering and elastic WorkerSets for both
:ref:`rollout and evaluation Workers <rolloutworker-reference-docs>`.
This provides fault tolerance at worker level.

This means that if you have rollout workers sitting on different machines and a 
machine is pre-empted, RLlib can continue training and evaluation with minimal interruption. 

The two properties that RLlib supports here are self-recovery and elasticity:

* **Elasticity**: RLlib continues training even when workers are removed. For example, if an RLlib trial uses spot instances, nodes may be removed from the cluster, potentially resulting in a subset of workers not getting scheduled. In this case, RLlib will continue with whatever healthy workers left at a reduced speed.
* **Self-Recovery**: When possible, RLlib will attempt to restore workers that were previously removed. During restoration, RLlib sync the latest state before new episodes can be sampled. 


Worker fault tolerance can be turned on by setting config ``recreate_failed_workers`` to True.

RLlib achieves this by utilizing a
`state-aware and fault tolerant actor manager <https://github.com/ray-project/ray/blob/master/rllib/utils/actor_manager.py>`__. Under the hood, RLlib relies on Ray Core :ref:`actor fault tolerance <actor-fault-tolerance>` to automatically recover failed worker actors.

Env Fault Tolerance
-------------------

In addition to worker fault tolerance, RLlib offers fault tolerance at environment level as well.

Rollout or evaluation workers will often run multiple environments in parallel to take
advantage of, for example, the parallel computing power that GPU offers. This can be controlled with
the ``num_envs_per_worker`` config. It may then be wasteful if the entire worker needs to be
reconstructed because of errors from a single environment.

In that case, RLlib offers the capability to restart individual environments without bubbling the
errors to higher level components. You can do that easily by turning on config
``restart_failed_sub_environments``.

.. note::
    Environment restarts are blocking.

    A rollout worker will wait until the environment comes back and finishes initialization.
    So for on-policy algorithms, it may be better to recover at worker level to make sure
    training progresses with elastic worker set while the environments are being reconstructed.
    More specifically, use configs ``num_envs_per_worker=1``, ``restart_failed_sub_environments=False``,
    and ``recreate_failed_workers=True``.
    

Fault Tolerance and Recovery Provided by Ray Tune
-------------------------------------------------

Ray Tune provides fault tolerance and recovery at the experiment trial level.

When using Ray Tune with RLlib, you can enable
:ref:`periodic checkpointing <rllib-saving-and-loading-algos-and-policies-docs>`,
which saves the state of the experiment to a user-specified persistent storage location.
If a trial fails, Ray Tune will automatically restart it from the latest
:ref:`checkpointed <tune-two-types-of-ckpt>` state.


Other Miscellaneous Considerations 
----------------------------------

By default, RLlib runs health checks during initial worker construction.
The whole job will error out if a completely healthy worker fleet can not be established
at the start of a training run. If an environment is by nature flaky, you may want to turn
off this feature by setting config ``validate_workers_after_construction`` to False.

Lastly, in an extreme case where no healthy workers are left for training, RLlib will wait
certain number of iterations for some of the workers to recover before the entire training
job failed.
The number of iterations it waits can be configured with the config
``num_consecutive_worker_failures_tolerance``.

..
    TODO(jungong) : move fault tolerance related options into a separate AlgorithmConfig
    group and update the doc here.
