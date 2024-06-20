"""Example using 2 ConnectorV2 for observation frame-stacking in Atari environments.

An RLlib Algorithm has 3 distinct connector pipelines:
- An env-to-module pipeline in an EnvRunner accepting a list of episodes and producing
a batch for an RLModule to compute actions (`forward_inference()` or
`forward_exploration()`).
- A module-to-env pipeline in an EnvRunner taking the RLModule's output and converting
it into an action readable by the environment.
- A learner connector pipeline on a Learner taking a list of episodes and producing
a batch for an RLModule to perform the training forward pass (`forward_train()`).

Each of these pipelines has a fixed set of default ConnectorV2 pieces that RLlib
adds/prepends to these pipelines in order to perform the most basic functionalities.
For example, RLlib adds the `AddObservationsFromEpisodesToBatch` ConnectorV2 into any
env-to-module pipeline to make sure the batch for computing actions contains - at the
minimum - the most recent observation.

On top of these default ConnectorV2 pieces, users can define their own ConnectorV2
pieces (or use the ones available already in RLlib) and add them to one of the 3
different pipelines described above, as required.

This example:
    - shows how the `FrameStackingEnvToModule` ConnectorV2 piece can be added to the
    env-to-module pipeline.
    - shows how the `FrameStackingLearner` ConnectorV2 piece can be added to the
    learner connector pipeline.
    - demonstrates that using these two pieces (rather than performing framestacking
    already inside the environment using a gymnasium wrapper) increases overall
    performance by about 5%.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack --num-frames=4 --env=ALE/Pong-v5`

Use the `--num-frames` option to define the number of observations to framestack.
If you don't want to use Connectors to perform the framestacking, set the
`--use-gym-wrapper-framestacking` flag to perform framestacking already inside a
gymnasium observation wrapper. In this case though, be aware that the tensors being
sent through the network are `--num-frames` x larger than if you use the Connector
setup.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------

With `--num-frames=4` and using the two extra ConnectorV2 pieces (in the env-to-module
and learner connector pipelines), you should see something like this using:
`--env ALE/Pong-v5 --num-gpus=4 --num-env-runners=95`
+---------------------------+------------+--------+------------------+...
| Trial name                | status     |   iter |   total time (s) |
|                           |            |        |                  |
|---------------------------+------------+--------+------------------+...
| PPO_atari-env_2fc4a_00000 | TERMINATED |    200 |          335.837 |
+---------------------------+------------+--------+------------------+...

Note that the time to run these 200 iterations is about ~5% faster than when
performing framestacking already inside the environment (using a
`gymnasium.wrappers.ObservationWrapper`), due to the additional network traffic
needed (sending back 4x[obs] batches instead of 1x[obs] to the learners).

Thus, with the `--use-gym-wrapper-framestacking` option (all other options being equal),
the output looks like this:
+---------------------------+------------+--------+------------------+...
| Trial name                | status     |   iter |   total time (s) |
|                           |            |        |                  |
|---------------------------+------------+--------+------------------+...
| PPO_atari-env_2fc4a_00000 | TERMINATED |    200 |          351.505 |
+---------------------------+------------+--------+------------------+...
"""
import gymnasium as gym

from ray.rllib.connectors.env_to_module.frame_stacking import FrameStackingEnvToModule
from ray.rllib.connectors.learner.frame_stacking import FrameStackingLearner
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.examples.envs.classes.multi_agent import make_multi_agent
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls

# Read in common example script command line arguments.
parser = add_rllib_example_script_args(
    default_timesteps=5000000, default_reward=20.0, default_iters=200
)
# Use Pong by default.
parser.set_defaults(env="ALE/Pong-v5")
parser.add_argument(
    "--num-frames",
    type=int,
    default=4,
    help="The number of observation frames to stack.",
)
parser.add_argument(
    "--use-gym-wrapper-framestacking",
    action="store_true",
    help="Whether to use RLlib's Atari wrapper's framestacking capabilities (as "
    "opposed to doing it via a specific ConenctorV2 pipeline).",
)


if __name__ == "__main__":
    from ray import tune

    args = parser.parse_args()

    assert (
        args.enable_new_api_stack
    ), "Must set --enable-new-api-stack when running this script!"

    # Define our custom connector pipelines.
    def _make_env_to_module_connector(env):
        # Create the env-to-module connector. We return an individual connector piece
        # here, which RLlib automatically integrates into a pipeline (and
        # add its default connector piece to the end of that pipeline).
        # The default pipeline automatically fixes the input- and output spaces of the
        # individual connector pieces in it.
        # Note that since the frame stacking connector does NOT write information
        # back to the episode (in order to save memory and network traffic), we
        # also need to perform the same procedure on the Learner end (see below
        # where we set up the Learner pipeline).
        return FrameStackingEnvToModule(
            num_frames=args.num_frames,
            multi_agent=args.num_agents > 0,
        )

    def _make_learner_connector(input_observation_space, input_action_space):
        # Create the learner connector.
        return FrameStackingLearner(
            num_frames=args.num_frames,
            multi_agent=args.num_agents > 0,
        )

    # Create a custom Atari setup (w/o the usual RLlib-hard-coded framestacking in it).
    # We would like our frame stacking connector to do this job.
    def _env_creator(cfg):
        return wrap_atari_for_new_api_stack(
            gym.make(args.env, **cfg, **{"render_mode": "rgb_array"}),
            # Perform framestacking either through ConnectorV2 or right here through
            # the observation wrapper.
            framestack=(
                args.num_frames if args.use_gym_wrapper_framestacking else None
            ),
        )

    if args.num_agents > 0:
        tune.register_env(
            "atari-env",
            lambda cfg: make_multi_agent(_env_creator)(
                dict(cfg, **{"num_agents": args.num_agents})
            ),
        )
    else:
        tune.register_env("atari-env", _env_creator)

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment(
            "atari-env",
            env_config={
                # Make analogous to old v4 + NoFrameskip.
                "frameskip": 1,
                "full_action_space": False,
                "repeat_action_probability": 0.0,
            },
            clip_rewards=True,
        )
        .env_runners(
            # ... new EnvRunner and our frame stacking env-to-module connector.
            env_to_module_connector=(
                None
                if args.use_gym_wrapper_framestacking
                else _make_env_to_module_connector
            ),
            num_envs_per_env_runner=1 if args.num_agents > 0 else 2,
        )
        .training(
            # Use our frame stacking learner connector.
            learner_connector=(
                None if args.use_gym_wrapper_framestacking else _make_learner_connector
            ),
            entropy_coeff=0.01,
            # Linearly adjust learning rate based on number of GPUs.
            lr=0.00015 * (args.num_gpus or 1),
            grad_clip=100.0,
            grad_clip_by="global_norm",
        )
        .rl_module(
            model_config_dict=dict(
                {
                    "vf_share_layers": True,
                    "conv_filters": [[16, 4, 2], [32, 4, 2], [64, 4, 2], [128, 4, 2]],
                    "conv_activation": "relu",
                    "post_fcnet_hiddens": [256],
                    "uses_new_env_runners": True,
                },
            )
        )
    )

    # PPO specific settings.
    if args.algo == "PPO":
        base_config.training(
            num_sgd_iter=10,
            mini_batch_size_per_learner=64,
            lambda_=0.95,
            kl_coeff=0.5,
            clip_param=0.1,
            vf_clip_param=10.0,
        )

    # Add a simple multi-agent setup.
    if args.num_agents > 0:
        base_config.multi_agent(
            policies={f"p{i}" for i in range(args.num_agents)},
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )

    # Run everything as configured.
    run_rllib_example_script_experiment(base_config, args)
