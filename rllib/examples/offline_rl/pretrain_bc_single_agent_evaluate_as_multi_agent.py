# @HybridAPIStack

"""Example showing how to train a (SA) BC RLModule while evaluating in a MA setup.

Here, SA=single-agent and MA=multi-agent.

Note that the BC Algorithm - by default - runs on the hybrid API stack, using RLModules,
but not `ConnectorV2` and `SingleAgentEpisode` yet.

This example:
    - demonstrates how you can train a single-agent BC Policy (RLModule) from a JSON
    file, which contains SampleBatch (expert or non-expert) data.
    - shows how you can run evaluation in a multi-agent setup (for example vs one
    or more heuristic policies), while training the BC Policy.


How to run this script
----------------------
`python [script file name].py --checkpoint-at-end`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
In the console output, you can see that the episode returns of the "main" policy on
the evaluation track keep increasing as BC manages to more and more clone the behavior
found in our (expert) JSON file.

After 50-100 iterations, you should see the episode reward reach 450.0.
Note that the opponent (random) policy does not learn as it's a) not a trainable
RLModule and b) not being trained via the BCConfig. It's only used for evaluation
purposes here.

+---------------------+------------+-----------------+--------+--------+
| Trial name          | status     | loc             |   iter |     ts |
|---------------------+------------+-----------------+--------+--------+
| BC_None_ee65e_00000 | TERMINATED | 127.0.0.1:35031 |     93 | 203754 |
+---------------------+------------+-----------------+--------+--------+
+----------------------+------------------------+
|   eps. return (main) |   eps. return (random) |
|----------------------+------------------------|
|                452.4 |                   28.3 |
+----------------------+------------------------+
"""
import os
from pathlib import Path

import gymnasium as gym

from ray import tune
from ray.air.constants import TRAINING_ITERATION
from ray.rllib.algorithms.bc import BCConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.examples._old_api_stack.policy.random_policy import RandomPolicy
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EVALUATION_RESULTS,
    NUM_ENV_STEPS_TRAINED,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.train.constants import TIME_TOTAL_S
from ray.tune.registry import register_env

parser = add_rllib_example_script_args(
    default_reward=450.0,
    default_timesteps=300000,
)
parser.set_defaults(num_agents=2)


if __name__ == "__main__":
    args = parser.parse_args()

    register_env("multi_cart", lambda cfg: MultiAgentCartPole(cfg))
    dummy_env = gym.make("CartPole-v1")

    rllib_dir = Path(__file__).parent.parent.parent
    print(f"rllib dir={rllib_dir}")
    offline_file = os.path.join(rllib_dir, "tests/data/cartpole/large.json")

    base_config = (
        BCConfig()
        # For offline RL, we do not specify an env here (b/c we don't want any env
        # instances created on the EnvRunners). Instead, we'll provide observation-
        # and action-spaces here for the RLModule to know its input- and output types.
        .environment(
            observation_space=dummy_env.observation_space,
            action_space=dummy_env.action_space,
        )
        .api_stack(
            enable_rl_module_and_learner=True,
        )
        .offline_data(
            input_=offline_file,
        )
        .multi_agent(
            policies={"main"},
            policy_mapping_fn=lambda *a, **kw: "main",
        )
        .evaluation(
            evaluation_interval=1,
            evaluation_num_env_runners=0,
            evaluation_config=BCConfig.overrides(
                # Evaluate on an actual env -> switch input back to "sampler".
                input_="sampler",
                # Do not explore during evaluation, but act greedily.
                explore=False,
                # Use a multi-agent setup for evaluation.
                env="multi_cart",
                env_config={"num_agents": args.num_agents},
                policies={
                    "main": PolicySpec(),
                    "random": PolicySpec(policy_class=RandomPolicy),
                },
                # Only control agent 0 with the main (trained) policy.
                policy_mapping_fn=(
                    lambda aid, *a, **kw: "main" if aid == 0 else "random"
                ),
                # Note that we do NOT have to specify the `policies_to_train` here,
                # b/c we are inside the evaluation config (no policy is trained during
                # evaluation). The fact that the BCConfig above is "only" setup
                # as single-agent makes it automatically only train the policy found in
                # the BCConfig's `policies` field (which is "main").
                # policies_to_train=["main"],
            ),
        )
    )

    policy_eval_returns = (
        f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/policy_reward_mean/"
    )

    stop = {
        # Check for the "main" policy's episode return, not the combined one.
        # The combined one is the sum of the "main" policy + the "random" one.
        policy_eval_returns + "main": args.stop_reward,
        NUM_ENV_STEPS_TRAINED: args.stop_timesteps,
        TRAINING_ITERATION: args.stop_iters,
    }

    run_rllib_example_script_experiment(
        base_config,
        args,
        stop=stop,
        success_metric={policy_eval_returns + "main": args.stop_reward},
        # We use a special progress reporter here to show the evaluation results (of the
        # "main" policy).
        # In the following dict, the keys are the (possibly nested) keys that can be
        # found in RLlib's (BC's) result dict, produced at every training iteration, and
        # the values are the column names you would like to see in your console reports.
        # Note that for nested result dict keys, you need to use slashes "/" to define
        # the exact path.
        progress_reporter=tune.CLIReporter(
            metric_columns={
                TRAINING_ITERATION: "iter",
                TIME_TOTAL_S: "total time (s)",
                NUM_ENV_STEPS_TRAINED: "ts",
                policy_eval_returns + "main": "eps. return (main)",
                policy_eval_returns + "random": "eps. return (random)",
            }
        ),
    )
