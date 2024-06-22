"""Example showing how to set up a custom progress reporter for an RLlib Algorithm.

The script sets the `progress_reporter` arg in the air.RunConfig and passes that to
Tune's Tuner:

```
tune.Tuner(
    param_space=...,  # <- your RLlib config
    run_config=air.RunConfig(
        callbacks=[some already instantiated TuneReporterBase object],
    ),
)
```

By default (progress_reporter=None), Tune will construct a default `CLIReporter` object,
which reports the episode mean return, number of env steps sampled and -trained, and
the total number of episodes run thus far.

NOTE that a custom progress reporter is different from a custom `Logger`, which defines,
how the (frequent) results are being formatted and written to e.g. a logfile.
To see an example on how to write your own Logger, see:
https://github.com/ray-project/ray/tree/master/rllib/examples/ray_tune/custom_logger.py


How to run this script
----------------------
`python [script file name].py


Results to expect
-----------------
You should see something similar to the following in your console output:

+---------------------+------------+-----------------+--------+------------------+
| Trial name          | status     | loc             |   iter |   total time (s) |
|---------------------+------------+-----------------+--------+------------------+
| PPO_env_bb503_00000 | TERMINATED | 127.0.0.1:26303 |      5 |          30.3823 |
+---------------------+------------+-----------------+--------+------------------+
+-------+-------------------+------------------+------------------+------------------+
|    ts |   combined return |   return policy1 |   return policy2 |   return policy3 |
|-------+-------------------+------------------+------------------+------------------|
| 20000 |             258.7 |            103.4 |            88.84 |            87.86 |
+-------+-------------------+------------------+------------------+------------------+

"""
from ray import air, tune
from ray.air.constants import TRAINING_ITERATION
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.tune.experimental.output import TrainReporter


my_multi_agent_progress_reporter = TrainReporter(
    verbosity=2,
    # In the following dict, the keys are the (possibly nested) keys that can be found
    # in RLlib's (PPO's) result dict, produced at every training iteration, and the
    # values are the actually displayed names you would like to see in your console
    # reports. Note that for nested result dict keys, you need to use slashes "/" to
    # define the exact path, do NOT use actually nested dicts in this `progress_metrics`
    # dict here.
    progress_metrics={
        **{
            TRAINING_ITERATION: "training iteration",
            "time_total_s": "total time (s)",
            NUM_ENV_STEPS_SAMPLED_LIFETIME: "env steps (lifetime)",
            # RLlib always sums up all agents' rewards and reports it under:
            # result_dict[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN].
            f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": "combined return",
        },
        # Because RLlib sums up all returns of all agents, we would like to also
        # see the individual agents' returns. We can find these under the result dict's
        # 'env_runners/module_episode_returns_mean/' key (then the policy ID):
        **{
            f"{ENV_RUNNER_RESULTS}/module_episode_returns_mean/{pid}": (
                f"return for policy {pid}"
            )
            for pid in ["policy1", "policy2", "policy3"]
        },
    },
)


if __name__ == "__main__":
    # Force Tuner to use old progress output as the new one silently ignores our custom
    # `CLIReporter`.
    # Register our multi-agent env with a fixed number of agents.
    # The agents' IDs are 0, 1, and 2.
    tune.register_env("env", lambda _: MultiAgentCartPole({"num_agents": 3}))

    config = (
        PPOConfig()
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
        .environment("env")
        .multi_agent(
            # Define 3 policies. Note that in our simple setup, they are all configured
            # the exact same way (with a PPO default RLModule/NN).
            policies={"policy1", "policy2", "policy3"},
            # Map agent 0 to "policy1", etc..
            policy_mapping_fn=lambda agent_id, episode: f"policy{agent_id + 1}",
        )
    )

    stop = {f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 200.0}

    # Run the actual experiment (using Tune).
    results = tune.Tuner(
        config.algo_class,
        param_space=config,
        run_config=air.RunConfig(
            stop=stop,
            # Plugin our own progress reporter as a tune callback (not to be confused
            # with RLlib callbacks!).
            callbacks=[my_multi_agent_progress_reporter],
        ),
    ).fit()
