"""An example script showing how to load module weights for 1 of n agents
from checkpoint.

This example:
    - Runs a multi-agent `Pendulum-v1` experiment with >= 2 policies.
    - Saves a checkpoint of the `MultiRLModule` used every `--checkpoint-freq`
       iterations.
    - Stops the experiments after the agents reach a combined return of -800.
    - Picks the best checkpoint by combined return and restores policy 0 from it.
    - Runs a second experiment with the restored `RLModule` for policy 0 and
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
single PPO policy (no tuning, just using RLlib's default settings). In the
second run of the experiment, the MultiRLModule weights for policy 0 are
restored from the checkpoint of the first run. The reward for a single agent
should be -400.0 again, but the training time should be shorter (around 30
iterations instead of 190).
"""

import os
from ray.air.constants import TRAINING_ITERATION
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentPendulum
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env

parser = add_rllib_example_script_args(
    default_iters=200,
    default_timesteps=100000,
    default_reward=-400.0,
)
parser.set_defaults(
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
    results = run_rllib_example_script_experiment(base_config, args)

    # Create an env instance to get the observation and action spaces.
    env = MultiAgentPendulum(config={"num_agents": args.num_agents})
    # Get the default module spec from the algorithm config.
    module_spec = base_config.get_default_rl_module_spec()
    module_spec.model_config = DefaultModelConfig(fcnet_activation="relu")
    module_spec.observation_space = env.envs[0].observation_space
    module_spec.action_space = env.envs[0].action_space
    # Create the module for each policy, but policy 0.
    module_specs = {}
    for i in range(1, args.num_agents or 1):
        module_specs[f"p{i}"] = module_spec

    # Now swap in the RLModule weights for policy 0.
    chkpt_path = results.get_best_result().checkpoint.path
    p_0_module_state_path = os.path.join(chkpt_path, "learner", "module_state", "p0")
    module_spec.load_state_path = p_0_module_state_path
    module_specs["p0"] = module_spec

    # Create the MultiRLModule.
    multi_rl_module_spec = MultiRLModuleSpec(rl_module_specs=module_specs)
    # Define the MultiRLModule in the base config.
    base_config.rl_module(rl_module_spec=multi_rl_module_spec)
    # We need to re-register the environment when starting a new run.
    register_env(
        "env",
        lambda _: MultiAgentPendulum(config={"num_agents": args.num_agents}),
    )
    # Define stopping criteria.
    stop = {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": -800,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 20000,
        TRAINING_ITERATION: 30,
    }

    # Run the experiment again with the restored MultiRLModule.
    run_rllib_example_script_experiment(base_config, args, stop=stop)
