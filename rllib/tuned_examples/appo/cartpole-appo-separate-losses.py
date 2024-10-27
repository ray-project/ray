# @OldAPIStack
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray import tune


stop = {
    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 400,
    f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000,
}

config = (
    APPOConfig()
    .environment("CartPole-v1")
    # Switch on >1 loss/optimizer API for TFPolicy and EagerTFPolicy.
    .experimental(_tf_policy_handles_more_than_one_loss=True)
    .training(
        # APPO will produce two separate loss terms: policy loss + value function loss.
        _separate_vf_optimizer=True,
        # Separate learning rate (and schedule) for the value function branch.
        _lr_vf=tune.grid_search([0.00075, [[0, 0.00075], [100000, 0.0003]]]),
        num_epochs=6,
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
