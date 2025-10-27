from ray.rllib.algorithms.impala import IMPALAConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.envs.classes.stateless_cartpole import StatelessCartPole
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_reward=350.0,
    default_timesteps=2000000,
)
parser.set_defaults(
    num_env_runners=5,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()


config = (
    IMPALAConfig()
    .environment(StatelessCartPole)
    # TODO (sven): Need to fix the MeanStdFilter(). It seems to cause NaNs when
    #  training.
    # .env_runners(
    #    env_to_module_connector=lambda env, spaces, device: MeanStdFilter(),
    # )
    .training(
        learner_queue_size=1,
        lr=0.0005 * ((args.num_learners or 1) ** 0.5),
        vf_loss_coeff=0.05,
        grad_clip=20.0,
        entropy_coeff=0.005,
    )
    .rl_module(
        model_config=DefaultModelConfig(
            vf_share_layers=True,
            use_lstm=True,
            max_seq_len=20,
        ),
    )
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
