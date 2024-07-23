from ray.rllib.algorithms.sac import SACConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentPendulum
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import add_rllib_example_script_args
from ray.tune.registry import register_env

torch, nn = try_import_torch()

parser = add_rllib_example_script_args()
parser.set_defaults(num_agents=2)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()

register_env(
    "multi_agent_pendulum",
    lambda _: MultiAgentPendulum({"num_agents": args.num_agents}),
)

config = (
    SACConfig()
    .environment(env="multi_agent_pendulum")
    .training(
        initial_alpha=1.001,
        lr=8e-4,
        target_entropy="auto",
        n_step=1,
        tau=0.005,
        train_batch_size_per_learner=256,
        target_network_update_freq=1,
        replay_buffer_config={
            "type": "MultiAgentPrioritizedEpisodeReplayBuffer",
            "capacity": 100000,
            "alpha": 1.0,
            "beta": 0.0,
        },
        num_steps_sampled_before_learning_starts=256,
    )
    .rl_module(
        model_config_dict={
            "fcnet_hiddens": [256, 256],
            "fcnet_activation": "tanh",
            "fcnet_weights_initializer": nn.init.xavier_uniform_,
            # "post_fcnet_hiddens": [],
            # "post_fcnet_activation": None,
            # "post_fcnet_weights_initializer": nn.init.orthogonal_,
            # "post_fcnet_weights_initializer_config": {"gain": 0.01},
        }
    )
    .reporting(
        metrics_num_episodes_for_smoothing=5,
        min_sample_timesteps_per_iteration=1000,
    )
)

if args.num_agents > 0:
    config.multi_agent(
        policy_mapping_fn=lambda aid, *arg, **kw: f"p{aid}",
        policies={f"p{i}" for i in range(args.num_agents)},
    )

stop = {
    NUM_ENV_STEPS_SAMPLED_LIFETIME: 500000,
    # `episode_return_mean` is the sum of all agents/policies' returns.
    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": -400.0 * args.num_agents,
}

if __name__ == "__main__":
    assert (
        args.num_agents > 0
    ), "The `--num-agents` arg must be > 0 for this script to work."
    assert (
        args.enable_new_api_stack
    ), "The `--enable-new-api-stack` arg must be activated for this script to work."

    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args, stop=stop)
