from ray.rllib.algorithms.impala import IMPALAConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_reward=450.0,
    default_timesteps=2000000,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()


config = (
    IMPALAConfig()
    .environment("CartPole-v1")
    .training(
        train_batch_size_per_learner=500,
        grad_clip=40.0,
        grad_clip_by="global_norm",
        lr=0.0005 * ((args.num_learners or 1) ** 0.5),
        vf_loss_coeff=0.05,
        entropy_coeff=0.0,
    )
    .rl_module(
        model_config=DefaultModelConfig(
            vf_share_layers=True,
        ),
    )
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
