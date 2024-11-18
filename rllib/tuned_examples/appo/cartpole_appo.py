from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_reward=450.0,
    default_timesteps=2000000,
)
parser.set_defaults(enable_new_api_stack=True)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()


config = (
    APPOConfig()
    .environment("CartPole-v1")
    .training(
        train_batch_size_per_learner=500,
        target_network_update_freq=2 * 4 * 2 * 500,  # 2n = 2*K*N
        vf_loss_coeff=0.01,
        entropy_coeff=0.007,
        circular_buffer_num_batches_N=4,
        circular_buffer_iterations_per_batch_K=2,
        grad_clip=30.0,
        lr=0.00075,
    )
    .rl_module(
        model_config=DefaultModelConfig(vf_share_layers=True),
    )
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
