.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

Fault Tolerance And Elastic Training
====================================

RLlib handles common failures modes, such as machine failures, spot instance preemption,
network outages, or Ray cluster failures.

There are three main areas for RLlib fault tolerance support:

* Worker recovery
* Environment fault tolerance
* Experiment level fault tolerance with Ray Tune


Worker Recovery
---------------

RLlib supports self-recovering and elastic :py:class:`~ray.rllib.env.env_runner_group.EnvRunnerGroup` for both
training and evaluation EnvRunner workers. This provides fault tolerance at worker level.

This means that if you have n :py:class:`~ray.rllib.env.env_runner.EnvRunner` workers sitting on different machines and a
machine is pre-empted, RLlib can continue training and evaluation with minimal interruption.

The two properties that RLlib supports here are self-recovery and elasticity:

* **Elasticity**: RLlib continues training even when it removes an :py:class:`~ray.rllib.env.env_runner.EnvRunner`. For example, if an RLlib trial uses spot instances, Ray may remove nodes from the cluster, potentially resulting in Ray not scheduling a subset of workers. In this case, RLlib continues with whatever healthy :py:class:`~ray.rllib.env.env_runner.EnvRunner` instances remain at a reduced speed.
* **Self-Recovery**: When possible, RLlib attempts to restore any :py:class:`~ray.rllib.env.env_runner.EnvRunner` that it previously removed. During restoration, RLlib syncs the latest state over to the restored :py:class:`~ray.rllib.env.env_runner.EnvRunner` before sampling new episodes.


You can turn on worker fault tolerance by setting ``config.fault_tolerance(restart_failed_env_runners=True)``.

RLlib achieves this by utilizing a
`state-aware and fault tolerant actor manager <https://github.com/ray-project/ray/blob/master/rllib/utils/actor_manager.py>`__. Under the hood, RLlib relies on Ray Core :ref:`actor fault tolerance <actor-fault-tolerance>` to automatically recover failed worker actors.

Env Fault Tolerance
-------------------

In addition to worker fault tolerance, RLlib offers fault tolerance at the environment level as well.

Rollout or evaluation workers often run multiple environments in parallel to take
advantage of, for example, the parallel computing power that GPU offers. You can control this parallelism with
the ``num_envs_per_env_runner`` config. It may then be wasteful if RLlib needs to reconstruct
the entire worker needs because of errors from a single environment.

In that case, RLlib offers the capability to restart individual environments without bubbling the
errors to higher level components. You can do that easily by turning on config
``restart_failed_sub_environments``.

.. note::
    Environment restarts are blocking.

    A rollout worker waits until the environment comes back and finishes initialization.
    So for on-policy algorithms, it may be better to recover at worker level to make sure
    training progresses with elastic worker set while RLlib reconstructs the environments.
    More specifically, use configs ``num_envs_per_env_runner=1``, ``restart_failed_sub_environments=False``,
    and ``restart_failed_env_runners=True``.


Fault Tolerance and Recovery Provided by Ray Tune
-------------------------------------------------

Ray Tune provides fault tolerance and recovery at the experiment trial level.

When using Ray Tune with RLlib, you can enable
:ref:`periodic checkpointing <rllib-checkpoints-docs>`,
which saves the state of the experiment to a user-specified persistent storage location.
If a trial fails, Ray Tune automatically restarts it from the latest
:ref:`checkpointed <tune-fault-tol>` state.


Other Miscellaneous Considerations
----------------------------------

By default, RLlib runs health checks during initial worker construction.
The whole job errors out if RLlib can't establish a completely healthy worker fleet
at the start of a training run. If an environment is by nature flaky, you may want to turn
off this feature by setting config ``validate_env_runners_after_construction`` to False.

Lastly, in an extreme case where no healthy workers remain for training, RLlib waits a
certain number of iterations for some of the workers to recover before the entire training
job fails.
You can configure the number of iterations RLlib waits with the config
``num_consecutive_env_runner_failures_tolerance``.

..
    TODO(jungong) : move fault tolerance related options into a separate AlgorithmConfig
    group and update the doc here.
