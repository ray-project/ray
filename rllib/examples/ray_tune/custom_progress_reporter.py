"""Example showing how to set up a custom progress reporter for an RLlib Algorithm.

The script sets the `progress_reporter` arg in the tune.RunConfig and passes that to
Tune's Tuner:

```
tune.Tuner(
    param_space=...,  # <- your RLlib config
    run_config=tune.RunConfig(
        progress_reporter=[some already instantiated TuneReporterBase object],
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
from ray import tune
from ray.tune.result import TRAINING_ITERATION
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)


my_multi_agent_progress_reporter = tune.CLIReporter(
    # In the following dict, the keys are the (possibly nested) keys that can be found
    # in RLlib's (PPO's) result dict, produced at every training iteration, and the
    # values are the column names you would like to see in your console reports.
    # Note that for nested result dict keys, you need to use slashes "/" to define the
    # exact path.
    metric_columns={
        **{
            TRAINING_ITERATION: "iter",
            "time_total_s": "total time (s)",
            NUM_ENV_STEPS_SAMPLED_LIFETIME: "ts",
            # RLlib always sums up all agents' rewards and reports it under:
            # result_dict[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN].
            f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": "combined return",
        },
        # Because RLlib sums up all returns of all agents, we would like to also
        # see the individual agents' returns. We can find these under the result dict's
        # 'env_runners/module_episode_returns_mean/' key (then the policy ID):
        **{
            f"{ENV_RUNNER_RESULTS}/module_episode_returns_mean/{pid}": f"return {pid}"
            for pid in ["policy1", "policy2", "policy3"]
        },
    },
)


if __name__ == "__main__":
    # Force Tuner to use old progress output as the new one silently ignores our custom
    # `CLIReporter`.
    # TODO (sven): Find out why we require this hack.
    import os

    os.environ["RAY_AIR_NEW_OUTPUT"] = "0"

    # Register our multi-agent env with a fixed number of agents.
    # The agents' IDs are 0, 1, and 2.
    tune.register_env("env", lambda _: MultiAgentCartPole({"num_agents": 3}))

    config = (
        PPOConfig()
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
        run_config=tune.RunConfig(
            stop=stop,
            verbose=2,
            # Plugin our own progress reporter.
            progress_reporter=my_multi_agent_progress_reporter,
        ),
    ).fit()
