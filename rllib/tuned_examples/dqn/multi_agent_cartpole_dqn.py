from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.tune.registry import register_env

from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_timesteps=500000,
)
parser.set_defaults(
    enable_new_api_stack=True,
    num_agents=2,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()

register_env("multi_agent_cartpole", lambda cfg: MultiAgentCartPole(config=cfg))

config = (
    DQNConfig()
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .environment(env="multi_agent_cartpole", env_config={"num_agents": args.num_agents})
    .training(
        lr=0.0005 * (args.num_gpus or 1) ** 0.5,
        train_batch_size_per_learner=48,
        replay_buffer_config={
            "type": "MultiAgentPrioritizedEpisodeReplayBuffer",
            "capacity": 50000,
            "alpha": 0.6,
            "beta": 0.4,
        },
        n_step=(2, 5),
        double_q=True,
        num_atoms=1,
        noisy=False,
        dueling=True,
    )
    .rl_module(
        model_config_dict={
            "fcnet_hiddens": [256, 256],
            "fcnet_activation": "tanh",
            "epsilon": [(0, 1.0), (20000, 0.02)],
            "fcnet_bias_initializer": "zeros_",
            "post_fcnet_bias_initializer": "zeros_",
            "post_fcnet_hiddens": [256],
        },
    )
)

if args.num_agents:
    config.multi_agent(
        policy_mapping_fn=lambda aid, *arg, **kw: f"p{aid}",
        policies={f"p{i}" for i in range(args.num_agents)},
    )

stop = {
    NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
    # `episode_return_mean` is the sum of all agents/policies' returns.
    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 200.0 * args.num_agents,
}

if __name__ == "__main__":

    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    assert (
        args.num_agents > 0
    ), "The `--num-agents` arg must be > 0 for this script to work."

    run_rllib_example_script_experiment(config, args, stop=stop)
