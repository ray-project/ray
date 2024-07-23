"""Example on how to compute actions in production on an already trained policy.

This example uses a more complex setup including a gymnasium environment, an
RLModule (one or more neural networks/policies), an env-to-module/module-to-env
ConnectorV2 pair, and an Episode object to store the ongoing episode in.
The RLModule contains an LSTM that requires its own previous STATE_OUT as new input
at every episode step to compute a new action.

This example:
    - shows how to use an already existing checkpoint to extract a single-agent RLModule
    from (our policy network).
    - shows how to setup this recovered policy net for action computations (with or
    without using exploration).
    - shows how to create a more complex env-loop in which the action-computing RLModule
    requires its own previous state outputs as new input and how to use RLlib's Episode
    APIs to achieve this.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack --stop-reward=200.0`

Use the `--explore-during-inference` option to switch on exploratory behavior
during inference. Normally, you should not explore during inference, though,
unless your environment has a stochastic optimal solution.
Use the `--num-episodes-during-inference=[int]` option to set the number of
episodes to run through during the inference phase using the restored RLModule.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

Note that the shown GPU settings in this script also work in case you are not
running via tune, but instead are using the `--no-tune` command line option.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`

You can visualize experiment results in ~/ray_results using TensorBoard.


Results to expect
-----------------

For the training step - depending on your `--stop-reward` setting, you should see
something similar to this:

Number of trials: 1/1 (1 TERMINATED)
+--------------------------------+------------+-----------------+--------+
| Trial name                     | status     | loc             |   iter |
|                                |            |                 |        |
|--------------------------------+------------+-----------------+--------+
| PPO_stateless-cart_cc890_00000 | TERMINATED | 127.0.0.1:72238 |      7 |
+--------------------------------+------------+-----------------+--------+
+------------------+------------------------+------------------------+
|   total time (s) |   num_env_steps_sample |   num_env_steps_traine |
|                  |             d_lifetime |             d_lifetime |
+------------------+------------------------+------------------------+
|          31.9655 |                  28000 |                  28000 |
+------------------+------------------------+------------------------+

Then, after restoring the RLModule for the inference phase, your output should
look similar to:

Training completed. Creating an env-loop for inference ...
Env ...
Env-to-module ConnectorV2 ...
RLModule restored ...
Module-to-env ConnectorV2 ...
Episode done: Total reward = 103.0
Episode done: Total reward = 90.0
Episode done: Total reward = 100.0
Episode done: Total reward = 111.0
Episode done: Total reward = 85.0
Episode done: Total reward = 90.0
Episode done: Total reward = 100.0
Episode done: Total reward = 102.0
Episode done: Total reward = 97.0
Episode done: Total reward = 81.0
Done performing action inference through 10 Episodes
"""
import os

from ray.rllib.connectors.env_to_module import (
    EnvToModulePipeline,
    AddObservationsFromEpisodesToBatch,
    AddStatesFromEpisodesToBatch,
    BatchIndividualItems,
    NumpyToTensor,
)
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.examples.envs.classes.stateless_cartpole import StatelessCartPole
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env

torch, _ = try_import_torch()


def _env_creator(cfg):
    return StatelessCartPole(cfg)


register_env("stateless-cart", _env_creator)


parser = add_rllib_example_script_args(default_reward=200.0)
parser.set_defaults(
    # Make sure that - by default - we produce checkpoints during training.
    checkpoint_freq=1,
    checkpoint_at_end=True,
    # Use StatelessCartPole by default.
    env="stateless-cart",
    # Script only runs on new API stack.
    enable_new_api_stack=True,
)
parser.add_argument(
    "--explore-during-inference",
    action="store_true",
    help="Whether the trained policy should use exploration during action "
    "inference.",
)
parser.add_argument(
    "--num-episodes-during-inference",
    type=int,
    default=10,
    help="Number of episodes to do inference over (after restoring from a checkpoint).",
)


if __name__ == "__main__":
    args = parser.parse_args()

    assert (
        args.enable_new_api_stack
    ), "Must set --enable-new-api-stack when running this script!"

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .training(
            num_sgd_iter=6,
            lr=0.0003,
            vf_loss_coeff=0.01,
        )
        # Add an LSTM setup to the default RLModule used.
        .rl_module(model_config_dict={"use_lstm": True})
    )

    print("Training LSTM-policy until desired reward/timesteps/iterations. ...")
    results = run_rllib_example_script_experiment(base_config, args)

    print("Training completed. Creating an env-loop for inference ...")

    print("Env ...")
    env = _env_creator(base_config.env_config)

    # We build the env-to-module pipeline here manually, but feel also free to build it
    # through the even easier:
    # `env_to_module = base_config.build_env_to_module_connector(env=env)`, which will
    # automatically add all default pieces necessary (for example the
    # `AddStatesFromEpisodesToBatch` component b/c we are using a stateful RLModule
    # here).
    print("Env-to-module ConnectorV2 ...")
    env_to_module = EnvToModulePipeline(
        input_observation_space=env.observation_space,
        input_action_space=env.action_space,
        connectors=[
            AddObservationsFromEpisodesToBatch(),
            AddStatesFromEpisodesToBatch(),
            BatchIndividualItems(multi_agent=args.num_agents > 0),
            NumpyToTensor(),
        ],
    )

    # Create the RLModule.
    # Get the last checkpoint from the above training run.
    best_result = results.get_best_result(
        metric=f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}", mode="max"
    )
    # Create new Algorithm and restore its state from the last checkpoint.
    rl_module = RLModule.from_checkpoint(
        os.path.join(
            best_result.checkpoint.path,
            "learner_group",
            "learner",
            "rl_module",
            DEFAULT_MODULE_ID,
        )
    )
    print("RLModule restored ...")

    # For the module-to-env pipeline, we will use the convenient config utility.
    print("Module-to-env ConnectorV2 ...")
    module_to_env = base_config.build_module_to_env_connector(env=env)

    # Now our setup is complete:
    # [gym.Env] -> env-to-module -> [RLModule] -> module-to-env -> [gym.Env] ... repeat
    num_episodes = 0

    obs, _ = env.reset()
    episode = SingleAgentEpisode(
        observations=[obs],
        observation_space=env.observation_space,
        action_space=env.action_space,
    )

    while num_episodes < args.num_episodes_during_inference:
        shared_data = {}
        input_dict = env_to_module(
            episodes=[episode],  # ConnectorV2 pipelines operate on lists of episodes.
            rl_module=rl_module,
            explore=args.explore_during_inference,
            shared_data=shared_data,
        )
        # No exploration.
        if not args.explore_during_inference:
            rl_module_out = rl_module.forward_inference(input_dict)
        # Using exploration.
        else:
            rl_module_out = rl_module.forward_exploration(input_dict)

        to_env = module_to_env(
            data=rl_module_out,
            episodes=[episode],  # ConnectorV2 pipelines operate on lists of episodes.
            rl_module=rl_module,
            explore=args.explore_during_inference,
            shared_data=shared_data,
        )
        # Send the computed action to the env. Note that the RLModule and the
        # connector pipelines work on batched data (B=1 in this case), whereas the Env
        # is not vectorized here, so we need to use `action[0]`.
        action = to_env.pop(Columns.ACTIONS)[0]
        obs, reward, terminated, truncated, _ = env.step(action)
        episode.add_env_step(
            obs,
            action,
            reward,
            terminated=terminated,
            truncated=truncated,
            # Same here: [0] b/c RLModule output is batched (w/ B=1).
            extra_model_outputs={k: v[0] for k, v in to_env.items()},
        )

        # Is the episode `done`? -> Reset.
        if episode.is_done:
            print(f"Episode done: Total reward = {episode.get_return()}")
            obs, info = env.reset()
            episode = SingleAgentEpisode(
                observations=[obs],
                observation_space=env.observation_space,
                action_space=env.action_space,
            )
            num_episodes += 1

    print(f"Done performing action inference through {num_episodes} Episodes")
