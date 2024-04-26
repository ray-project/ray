from ray.rllib.algorithms.impala import ImpalaConfig


stop = {
    "sampler_results/episode_reward_mean": 150,
    "timesteps_total": 200000,
}

config = (
    ImpalaConfig()
    .environment("CartPole-v1")
    # Switch on >1 loss/optimizer API for TFPolicy and EagerTFPolicy.
    .experimental(_tf_policy_handles_more_than_one_loss=True)
    .training(
        # IMPALA will produce two separate loss terms: policy loss + value function
        # loss.
        _separate_vf_optimizer=True,
        # Separate learning rate for the value function branch.
        _lr_vf=0.00075,
        num_sgd_iter=6,
        # `vf_loss_coeff` will be ignored anyways as we use separate loss terms.
        vf_loss_coeff=0.01,
        vtrace=True,
        model={
            # Make sure we really have completely separate branches.
            "vf_share_layers": False,
        },
    )
    .env_runners(
        num_envs_per_env_runner=5,
        num_env_runners=1,
        observation_filter="MeanStdFilter",
    )
    .resources(num_gpus=0)
)
