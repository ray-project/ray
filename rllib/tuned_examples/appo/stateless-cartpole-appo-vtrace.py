from ray.rllib.algorithms.appo.appo import APPOConfig
from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole


config = (
    APPOConfig()
    .environment(StatelessCartPole)
    .resources(num_gpus=0)
    .rollouts(num_rollout_workers=0, observation_filter="MeanStdFilter")#TODO rollout workers=5
    .training(lr=0.0003, num_sgd_iter=6, vf_loss_coeff=0.01, model={
        "fcnet_hiddens": [32],
        "fcnet_activation": "linear",
        "vf_share_layers": True,
        "use_lstm": True,
    })
)

stop = {
    "timesteps_total": 500000,
    "sampler_results/episode_reward_mean": 150.0,
}
