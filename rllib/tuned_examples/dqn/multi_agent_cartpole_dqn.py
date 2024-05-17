from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import add_rllib_example_script_args
from ray.tune.registry import register_env

parser = add_rllib_example_script_args()
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values toset up `config` below.
args = parser.parse_args()

register_env(
    "multi_agent_cartpole", lambda _: MultiAgentCartPole({"num_agents": 2})
)

config = (
    DQNConfig()
    .environment(env="multi_agent_cartpole")
    .rl_module(
        model_config_dict={
            "fcnet_hiddens": [256],
            "fcnet_activation": "relu",
            "epsilon": [(0, 1.0), (10000, 0.02)],
            "fcnet_bias_initializer": "zeros_",
            "post_fcnet_bias_initializer": "zeros_",
            "post_fcnet_hiddens": [256],
        },
    )
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .env_runners(
        rollout_fragment_length=1,
        num_env_runners=2,
        num_envs_per_env_runner=1,
    )
    .training(
        double_q=True,
        num_atoms=1,
        noisy=False,
        dueling=True,
        replay_buffer_config={
            "type": "MultiAgentPrioritizedEpisodeReplayBuffer",
            "capacity": 100000,
            "alpha": 0.6,
            "beta": 0.4,
        },
    )
    .reporting(
        metrics_num_episodes_for_smoothing=5,
        min_sample_timesteps_per_iteration=1000,
    )
    .multi_agent(
        policy_mapping_fn=lambda aid, *arg, **kw: f"p{aid}",
        policies={"p0", "p1"},
    )
    .debugging(
        log_level="DEBUG",
    )
)

stop = {
    NUM_ENV_STEPS_SAMPLED_LIFETIME: 500000,
    # `episode_return_mean` is the sum of all agents/policies' returns.
    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 450.0 * 2,
}

if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args, stop=stop)
