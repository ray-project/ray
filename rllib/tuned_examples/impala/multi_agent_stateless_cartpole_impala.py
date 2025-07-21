from ray.rllib.algorithms.impala import IMPALAConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentStatelessCartPole
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import add_rllib_example_script_args
from ray.tune.registry import register_env

parser = add_rllib_example_script_args(default_timesteps=5000000)
parser.set_defaults(
    num_agents=2,
    num_env_runners=4,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()

register_env(
    "multi_stateless_cart",
    lambda cfg: MultiAgentStatelessCartPole(config=cfg),
)


config = (
    IMPALAConfig()
    .environment("multi_stateless_cart", env_config={"num_agents": args.num_agents})
    # TODO (sven): Need to fix the MeanStdFilter(). It seems to cause NaNs when
    #  training.
    # .env_runners(
    #    env_to_module_connector=lambda env, spaces, device: MeanStdFilter(multi_agent=True),
    # )
    .training(
        train_batch_size_per_learner=600,
        lr=0.0005 * ((args.num_learners or 1) ** 0.5),
        vf_loss_coeff=0.05,
        grad_clip=20.0,
        entropy_coeff=0.02,
    )
    .rl_module(
        model_config=DefaultModelConfig(
            vf_share_layers=True,
            use_lstm=True,
            max_seq_len=20,
        ),
    )
    .multi_agent(
        policy_mapping_fn=(lambda agent_id, episode, **kwargs: f"p{agent_id}"),
        policies={f"p{i}" for i in range(args.num_agents)},
    )
)

stop = {
    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 150.0 * args.num_agents,
    NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
}


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args, stop=stop)
