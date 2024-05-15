# @OldAPIStack
from ray.rllib.algorithms.appo.appo import APPOConfig
from ray.rllib.examples.envs.classes.stateless_cartpole import StatelessCartPole
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)


config = (
    APPOConfig()
    # TODO: Switch over to new stack once it supports LSTMs.
    .api_stack(enable_rl_module_and_learner=False)
    .environment(StatelessCartPole)
    .resources(num_gpus=0)
    .env_runners(num_env_runners=1, observation_filter="MeanStdFilter")
    .training(
        lr=0.0003,
        num_sgd_iter=6,
        vf_loss_coeff=0.01,
        model={
            "fcnet_hiddens": [32],
            "fcnet_activation": "linear",
            "vf_share_layers": True,
            "use_lstm": True,
        },
    )
)

stop = {
    f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 500000,
    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 150.0,
}
