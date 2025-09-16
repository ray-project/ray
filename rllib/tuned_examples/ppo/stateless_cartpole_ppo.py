from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.env_to_module import MeanStdFilter
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.envs.classes.stateless_cartpole import StatelessCartPole
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_timesteps=2000000,
    default_reward=350.0,
)
parser.set_defaults(
    num_env_runners=3,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()

config = (
    PPOConfig()
    .environment(StatelessCartPole)
    .env_runners(
        env_to_module_connector=lambda env, spaces, device: MeanStdFilter(),
    )
    .training(
        lr=0.0003 * ((args.num_learners or 1) ** 0.5),
        num_epochs=6,
        vf_loss_coeff=0.05,
    )
    .rl_module(
        model_config=DefaultModelConfig(
            vf_share_layers=False,
            use_lstm=True,
            max_seq_len=20,
        ),
    )
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
