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

""" Example using connectors (V2) for frame-stacking in Atari environments.

How to run this script
----------------------
`python [script file name].py --enable-new-api-stack`
For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.
For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`
"""

# Read in common example script command line arguments.
parser = add_rllib_example_script_args(
    default_timesteps=5000000, default_reward=20.0, default_iters=200
)
parser.add_argument(
    "--atari-env",
    type=str,
    default="ALE/Pong-v5",
    help="The name of the Atari env to run, e.g. `ALE/Breakout-v5`.",
)
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

    # Define our custom connector pipelines.
    def _make_env_to_module_connector(env):
        # Create the env-to-module connector. We return an individual connector piece
        # here, which RLlib will then automatically integrate into a pipeline (and
        # add its default connector piece to the end of that pipeline).
        # This pipeline also automatically fixes the input- and output spaces of the
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
            gym.make(args.atari_env, **cfg, **{"render_mode": "rgb_array"}),
            # Perform framestacking either through ConnectorV2 or right here through
            # the observation wrapper.
            framestack=(
                args.num_framestack if args.use_gym_wrapper_framestacking else None
            ),
        )

    if args.num_agents > 0:
        tune.register_env(
            "env",
            lambda cfg: make_multi_agent(_env_creator)(
                dict(cfg, **{"num_agents": args.num_agents})
            ),
        )
    else:
        tune.register_env("env", _env_creator)

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment(
            "env",
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
            lambda_=0.95,
            kl_coeff=0.5,
            clip_param=0.1,
            vf_clip_param=10.0,
            entropy_coeff=0.01,
            num_sgd_iter=10,
            # Linearly adjust learning rate based on number of GPUs.
            lr=0.00015 * (args.num_gpus or 1),
            grad_clip=100.0,
            grad_clip_by="global_norm",
        )
    )
    if args.enable_new_api_stack:
        base_config = base_config.rl_module(
            model_config_dict=dict(
                {
                    "vf_share_layers": True,
                    "conv_filters": [[16, 4, 2], [32, 4, 2], [64, 4, 2], [128, 4, 2]],
                    "conv_activation": "relu",
                    "post_fcnet_hiddens": [256],
                    "uses_new_env_runners": True,
                },
            ),
        )
    else:
        base_config = base_config.training(
            {
                "vf_share_layers": True,
                "conv_filters": [[16, 4, 2], [32, 4, 2], [64, 4, 2], [128, 4, 2]],
                "conv_activation": "relu",
                "post_fcnet_hiddens": [256],
                "uses_new_env_runners": False,
            }
        )

    # Add a simple multi-agent setup.
    if args.num_agents > 0:
        base_config.multi_agent(
            policies={f"p{i}" for i in range(args.num_agents)},
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )

    # Run everything as configured.
    run_rllib_example_script_experiment(base_config, args)
