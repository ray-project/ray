---
myst:
  html_meta:
    description: "Fault tolerance and elastic training in RLlib: EnvRunner recovery, environment failures, and how Ray Tune restores interrupted experiments."
---

# Fault tolerance and elastic training

RLlib handles common failure modes, such as machine failures, spot instance preemption, network outages, or Ray cluster failures.

RLlib supports fault tolerance in three areas:

* Worker recovery
* Environment fault tolerance
* Experiment-level fault tolerance with Ray Tune

## Worker recovery

RLlib supports self-recovering and elastic {py:class}`~ray.rllib.env.env_runner_group.EnvRunnerGroup` for both training and evaluation EnvRunner workers. This provides fault tolerance at the worker level.

If you have n {py:class}`~ray.rllib.env.env_runner.EnvRunner` workers on different machines and Ray preempts one machine, RLlib continues training and evaluation with minimal interruption.

RLlib supports two properties here, self-recovery and elasticity:

* **Elasticity**: RLlib continues training even when it removes an {py:class}`~ray.rllib.env.env_runner.EnvRunner`. For example, if an RLlib trial uses spot instances, Ray might remove nodes from the cluster and fail to schedule a subset of workers. RLlib then continues at a reduced speed with whatever healthy {py:class}`~ray.rllib.env.env_runner.EnvRunner` instances remain.
* **Self-recovery**: When possible, RLlib restores any {py:class}`~ray.rllib.env.env_runner.EnvRunner` that it previously removed. During restoration, RLlib syncs the latest state to the restored {py:class}`~ray.rllib.env.env_runner.EnvRunner` before sampling new episodes.

Turn on worker fault tolerance by setting `config.fault_tolerance(restart_failed_env_runners=True)`.

RLlib achieves this with a [state-aware and fault-tolerant actor manager](https://github.com/ray-project/ray/blob/master/rllib/utils/actor_manager.py). It relies on Ray Core {ref}`actor fault tolerance <actor-fault-tolerance>` to automatically recover failed worker actors.

## Environment fault tolerance

RLlib also offers fault tolerance at the environment level.

Rollout or evaluation workers often run multiple environments in parallel, for example to use the parallel computing power that a GPU offers. Control this parallelism with the `num_envs_per_env_runner` config. Reconstructing the entire worker because of errors from a single environment can be wasteful.

Instead, RLlib can restart individual environments without bubbling the errors up to higher-level components. Turn this on with the `restart_failed_sub_environments` config.

:::{note}
Environment restarts are blocking.

A rollout worker waits until the environment comes back and finishes initialization. For on-policy algorithms, recovering at the worker level might be better, so training progresses with an elastic worker set while RLlib reconstructs the environments. Set `num_envs_per_env_runner=1`, `restart_failed_sub_environments=False`, and `restart_failed_env_runners=True`.
:::

## Fault tolerance and recovery with Ray Tune

Ray Tune provides fault tolerance and recovery at the experiment-trial level.

When you use Ray Tune with RLlib, you can enable {ref}`periodic checkpointing <rllib-checkpoints-docs>`, which saves the experiment state to a persistent storage location you specify. If a trial fails, Ray Tune automatically restarts it from the latest {ref}`checkpointed <tune-fault-tol>` state.

## Other considerations

By default, RLlib runs health checks during initial worker construction. The whole job errors out if RLlib can't establish a healthy worker fleet at the start of a training run. If an environment is flaky by nature, turn off this check by setting the `validate_env_runners_after_construction` config to `False`.

If no healthy workers remain for training, RLlib waits a number of iterations for some workers to recover before the entire training job fails. Configure the number of iterations with the `num_consecutive_env_runner_failures_tolerance` config.

<!-- TODO(jungong) : move fault tolerance related options into a separate AlgorithmConfig
group and update the doc here. -->
