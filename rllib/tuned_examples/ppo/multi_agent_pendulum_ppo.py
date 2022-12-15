from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.examples.env.multi_agent import MultiAgentPendulum
from ray.tune.registry import register_env


register_env("multi_agent_pendulum", lambda _: MultiAgentPendulum({"num_agents": 1}))

config = (
    PPOConfig()
    .environment("multi_agent_pendulum")
    .framework("torch")
    .rollouts(
        num_envs_per_worker=10, batch_mode="complete_episodes", enable_connectors=True
    )
    .training(
        train_batch_size=2048,
        lambda_=0.1,
        grad_clip=0.95,
        lr=0.0003,
        sgd_minibatch_size=64,
        num_sgd_iter=10,
        model={"fcnet_hiddens": [128, 128]},
        vf_clip_param=10.0,
    )
    .rl_module(_enable_rl_module_api=True)
)

stop = {
    "timesteps_total": 500000,
    "episode_reward_mean": -400.0,
}
