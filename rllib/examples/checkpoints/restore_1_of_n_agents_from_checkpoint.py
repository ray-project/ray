"""Example demonstrating how to load module weights for 1 of n agents from a checkpoint.

This example:
    - Runs a multi-agent `Pendulum-v1` experiment with >= 2 policies, p0, p1, etc..
    - Saves a checkpoint of the `MultiRLModule` every `--checkpoint-freq`
    iterations.
    - Stops the experiments after the agents reach a combined return of -800.
    - Picks the best checkpoint by combined return and restores p0 from it.
    - Runs a second experiment with the restored `RLModule` for p0 and
    a fresh `RLModule` for the other policies.
    - Stops the second experiment after the agents reach a combined return of -800.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack --num-agents=2
--checkpoint-freq=20 --checkpoint-at-end`

Control the number of agents and policies (RLModules) via --num-agents and
--num-policies.

Control the number of checkpoints by setting `--checkpoint-freq` to a value > 0.
Note that the checkpoint frequency is per iteration and this example needs at
least a single checkpoint to load the RLModule weights for policy 0.
If `--checkpoint-at-end` is set, a checkpoint will be saved at the end of the
experiment.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
You should expect a reward of -400.0 eventually being achieved by a simple
single PPO policy. In the second run of the experiment, the MultiRLModule weights
for policy 0 are restored from the checkpoint of the first run. The reward for a
single agent should be -400.0 again, but the training time should be shorter
(around 30 iterations instead of 190) due to the fact that one policy is already
an expert from the get go.
"""

from pathlib import Path

from ray.tune.result import TRAINING_ITERATION
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.core import (
    COMPONENT_LEARNER,
    COMPONENT_LEARNER_GROUP,
    COMPONENT_RL_MODULE,
)
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentPendulum
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    check,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env

parser = add_rllib_example_script_args(
    # Pendulum-v1 sum of 2 agents (each agent reaches -250).
    default_reward=-500.0,
)
parser.set_defaults(
    enable_new_api_stack=True,
    checkpoint_freq=1,
    num_agents=2,
)
# TODO (sven): This arg is currently ignored (hard-set to 2).
parser.add_argument("--num-policies", type=int, default=2)


if __name__ == "__main__":
    args = parser.parse_args()

    # Register our environment with tune.
    if args.num_agents > 1:
        register_env(
            "env",
            lambda _: MultiAgentPendulum(config={"num_agents": args.num_agents}),
        )
    else:
        raise ValueError(
            f"`num_agents` must be > 1, but is {args.num_agents}."
            "Read the script docstring for more information."
        )

    assert args.checkpoint_freq > 0, (
        "This example requires at least one checkpoint to load the RLModule "
        "weights for policy 0."
    )

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment("env")
        .training(
            train_batch_size_per_learner=512,
            minibatch_size=64,
            lambda_=0.1,
            gamma=0.95,
            lr=0.0003,
            vf_clip_param=10.0,
        )
        .rl_module(
            model_config=DefaultModelConfig(fcnet_activation="relu"),
        )
    )

    # Add a simple multi-agent setup.
    if args.num_agents > 0:
        base_config.multi_agent(
            policies={f"p{i}" for i in range(args.num_agents)},
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )

    # Augment the base config with further settings and train the agents.
    results = run_rllib_example_script_experiment(base_config, args, keep_ray_up=True)

    # Now swap in the RLModule weights for policy 0.
    chkpt_path = results.get_best_result().checkpoint.path
    p_0_module_state_path = (
        Path(chkpt_path)  # <- algorithm's checkpoint dir
        / COMPONENT_LEARNER_GROUP  # <- learner group
        / COMPONENT_LEARNER  # <- learner
        / COMPONENT_RL_MODULE  # <- MultiRLModule
        / "p0"  # <- (single) RLModule
    )

    class LoadP0OnAlgoInitCallback(DefaultCallbacks):
        def on_algorithm_init(self, *, algorithm, **kwargs):
            module_p0 = algorithm.get_module("p0")
            weight_before = convert_to_numpy(next(iter(module_p0.parameters())))
            algorithm.restore_from_path(
                p_0_module_state_path,
                component=(
                    COMPONENT_LEARNER_GROUP
                    + "/"
                    + COMPONENT_LEARNER
                    + "/"
                    + COMPONENT_RL_MODULE
                    + "/p0"
                ),
            )
            # Make sure weights were updated.
            weight_after = convert_to_numpy(next(iter(module_p0.parameters())))
            check(weight_before, weight_after, false=True)

    base_config.callbacks(LoadP0OnAlgoInitCallback)

    # Define stopping criteria.
    stop = {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": -800.0,
        f"{ENV_RUNNER_RESULTS}/{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 100000,
        TRAINING_ITERATION: 100,
    }

    # Run the experiment again with the restored MultiRLModule.
    run_rllib_example_script_experiment(base_config, args, stop=stop)
