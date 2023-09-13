from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.examples.env.multi_agent import MultiAgentPendulum
from ray.tune.registry import register_env


register_env("multi_agent_pendulum", lambda _: MultiAgentPendulum({"num_agents": 1}))

config = (
    PPOConfig()
    .environment("multi_agent_pendulum")
    .rollouts(
        num_envs_per_worker=20,
        observation_filter="MeanStdFilter",
        num_rollout_workers=0,
    )
    .training(
        train_batch_size=512,
        lambda_=0.1,
        gamma=0.95,
        lr=0.0003,
        sgd_minibatch_size=64,
        model={"fcnet_activation": "relu"},
        vf_clip_param=10.0,
    )
)

stop = {
    "timesteps_total": 500000,
    "episode_reward_mean": -400.0,
}
