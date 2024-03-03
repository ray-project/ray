import numpy as np

from ray.rllib.connectors.module_to_env.mixin_heuristic_actions import (
    MixinHeuristicActions,
)
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls


# Read in common example script command line arguments.
parser = add_rllib_example_script_args(
    default_timesteps=20000, default_reward=500.0, default_iters=100
)
parser.add_argument(
    "--mixin-weight",
    type=float,
    default=0.5,
    help="The weight between 0.0 and 1.0 of the heuristic action mixin (over the "
    "RLModule computed one). 0.0 means only use our trained RLModule, 1.0 means only "
    "use the heuristic policy.",
)


def cartpole_perfect_actions(obs):
    cart_pos, cart_vel, pole_angle, pole_vel = obs

    # The following tuning parameters should provide almost perfect control that can
    # reliably reach a 500.0 return in CartPole-v1.
    k_angle = 0.75  # Weight for how much we care about the pole angle.
    k_pos = 0.5  # Weight for how much we care about the cart position.
    k_vel = 0.5  # Weight for how much we care about the cart velocity.
    k_pole_vel = 0.5  # Weight for how much we care about the pole velocity at its tip.
    # Calculate a simple score that represents the desirability of moving right
    move_right_score = (
        pole_angle * k_angle
        + pole_vel * k_pole_vel
        + cart_pos * k_pos
        + cart_vel * k_vel
    )
    # Decide on action based on the score.
    if move_right_score > 0:
        return np.array([0.0, 1.0])
    else:
        return np.array([1.0, 0.0])


if __name__ == "__main__":
    from ray import tune

    args = parser.parse_args()

    if args.num_agents > 0:
        tune.register_env(
            "env",
            lambda cfg: MultiAgentCartPole(
                dict(cfg, **{"num_agents": args.num_agents})
            ),
        )

    config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        # Use new API stack ...
        .experimental(_enable_new_api_stack=args.enable_new_api_stack)
        .framework(args.framework)
        .environment("env" if args.num_agents > 0 else "CartPole-v1")
        .rollouts(
            num_rollout_workers=args.num_env_runners,
            # Set up the correct env-runner to use depending on
            # old-stack/new-stack and multi-agent settings.
            env_runner_cls=(
                None
                if not args.enable_new_api_stack
                else SingleAgentEnvRunner
                if args.num_agents == 0
                else MultiAgentEnvRunner
            ),
        )
        .resources(
            num_gpus=args.num_gpus,  # old stack
            num_learner_workers=args.num_gpus,  # new stack
            num_gpus_per_learner_worker=1 if args.num_gpus else 0,
            num_cpus_for_local_worker=1,
        )
        .training(
            gamma=0.99,
            lr=0.0003,
            model=dict(
                {
                    "vf_share_layers": True,
                    "fcnet_hiddens": [32],
                    "fcnet_activation": "linear",
                },
                **({"uses_new_env_runners": True} if args.enable_new_api_stack else {}),
            ),
        )
        .evaluation(
            evaluation_interval=1,
            evaluation_num_workers=2,
            enable_async_evaluation=True,
            evaluation_duration=10,
            evaluation_config={
                "explore": False,
                "_module_to_env_connector": lambda env: MixinHeuristicActions(
                    compute_heuristic_actions=cartpole_perfect_actions,
                    mixin_weight=args.mixin_weight,
                ),
            },
        )
    )

    # Add a simple multi-agent setup.
    if args.num_agents > 0:
        config.multi_agent(
            policies={f"p{i}" for i in range(args.num_agents)},
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )

    # Run everything as configured.
    stop = {
        "evaluation/sampler_results/episode_reward_mean": 500.0,
    }
    run_rllib_example_script_experiment(config, args, stop=stop)
