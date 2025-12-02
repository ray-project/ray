from ray.rllib.algorithms.impala import IMPALAConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import add_rllib_example_script_args
from ray.tune.registry import register_env

parser = add_rllib_example_script_args()
parser.set_defaults(
    num_agents=2,
    num_env_runners=4,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()

register_env("multi_cart", lambda cfg: MultiAgentCartPole(config=cfg))


config = (
    IMPALAConfig()
    .environment("multi_cart", env_config={"num_agents": args.num_agents})
    .training(
        train_batch_size_per_learner=1000,
        grad_clip=30.0,
        grad_clip_by="global_norm",
        lr=0.0005,
        vf_loss_coeff=0.01,
        entropy_coeff=0.0,
    )
    .rl_module(
        model_config=DefaultModelConfig(
            vf_share_layers=True,
        ),
    )
    .multi_agent(
        policy_mapping_fn=(lambda agent_id, episode, **kwargs: f"p{agent_id}"),
        policies={f"p{i}" for i in range(args.num_agents)},
    )
)

stop = {
    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 350.0 * args.num_agents,
    NUM_ENV_STEPS_SAMPLED_LIFETIME: 2500000,
}


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args, stop=stop)
