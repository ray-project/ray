from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_reward=450.0, default_timesteps=1_000_000
)

args = parser.parse_args()

config = (
    PPOConfig()
    .environment("CartPole-v1")
    .training(
        lr=0.0003,
        num_epochs=6,
        vf_loss_coeff=0.01,
    )
    .rl_module(
        model_config=DefaultModelConfig(
            fcnet_hiddens=[32],
            fcnet_activation="linear",
            vf_share_layers=True,
        ),
    )
    .env_runners(
        use_inference_actors=True,
    )
    .inference(
        num_inference_actors=1,
        inference_num_cpus_per_actor=1,
        inference_num_gpus_per_actor=0,
    )
)


if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
