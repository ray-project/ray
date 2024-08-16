from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.tune.registry import register_env

from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args()
parser.set_defaults(num_agents=2)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()
parser.set_defaults(num_agents=2)
register_env(
    "multi_agent_cartpole",
    lambda _: MultiAgentCartPole({"num_agents": args.num_agents}),
)

config = (
    DQNConfig()
    .environment(env="multi_agent_cartpole")
    .training(
        # Settings identical to old stack.
        train_batch_size_per_learner=32,
        replay_buffer_config={
            "type": "MultiAgentPrioritizedEpisodeReplayBuffer",
            "capacity": 50000,
            "alpha": 0.6,
            "beta": 0.4,
        },
        n_step=3,
        double_q=True,
        num_atoms=1,
        noisy=False,
        dueling=True,
    )
    .rl_module(
        model_config_dict={
            "fcnet_hiddens": [256],
            "fcnet_activation": "tanh",
            "epsilon": [(0, 1.0), (10000, 0.02)],
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
    NUM_ENV_STEPS_SAMPLED_LIFETIME: 500000,
    # `episode_return_mean` is the sum of all agents/policies' returns.
    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 250.0 * args.num_agents,
}

if __name__ == "__main__":
    assert (
        args.num_agents > 0
    ), "The `--num-agents` arg must be > 0 for this script to work."
    assert (
        args.enable_new_api_stack
    ), "The `--enable-new-api-stack` arg must be activated for this script to work."

    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args, stop=stop)
