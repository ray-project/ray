from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner

config = (
    DQNConfig()
    .environment(env="CartPole-v1")
    .framework(framework="torch")
    .experimental(_enable_new_api_stack=True)
    .rollouts(
        env_runner_cls=SingleAgentEnvRunner,
    )
    .training(
        model={
            "fcnet_hiddens": [64],
            "fcnet_activation": "linear",
        },
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
            "capacity": 50000,
            "alpha": 0.6,
            "beta": 0.4,
        },
        double_q=False,
        num_atoms=1,
        dueling=False,
        noisy=True,
        sigma0=0.2,
    )
)

stop = {
    "sampler_results/episode_reward_mean": 100,
    "timesteps_total": 10000,
}
