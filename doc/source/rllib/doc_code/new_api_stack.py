# __enabling-new-api-stack-sa-ppo-begin__

from ray.rllib.algorithms.ppo import PPOConfig


config = (
    PPOConfig()
    .environment("CartPole-v1")
    # Switch both the new API stack flags to True (both False by default).
    # This enables the use of
    # a) RLModule (replaces ModelV2) and Learner (replaces Policy)
    # b) and automatically picks the correct EnvRunner (single-agent vs multi-agent)
    # and enables ConnectorV2 support.
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .resources(
        num_cpus_for_main_process=1,
    )
    # We are using a simple 1-CPU setup here for learning. However, as the new stack
    # supports arbitrary scaling on the learner axis, feel free to set
    # `num_learners` to the number of available GPUs for multi-GPU training (and
    # `num_gpus_per_learner=1`).
    .learners(
        num_learners=0,  # <- in most cases, set this value to the number of GPUs
        num_gpus_per_learner=0,  # <- set this to 1, if you have at least 1 GPU
    )
    # When using RLlib's default models (RLModules) AND the new EnvRunners, you should
    # set this flag in your model config. Having to set this, will no longer be required
    # in the near future. It does yield a small performance advantage as value function
    # predictions for PPO are no longer required to happen on the sampler side (but are
    # now fully located on the learner side, which might have GPUs available).
    .training(model={"uses_new_env_runners": True})
)

# __enabling-new-api-stack-sa-ppo-end__

# Test whether it works.
print(config.build().train())


# __enabling-new-api-stack-ma-ppo-begin__

from ray.rllib.algorithms.ppo import PPOConfig  # noqa
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole  # noqa


# A typical multi-agent setup (otherwise using the exact same parameters as before)
# looks like this.
config = (
    PPOConfig()
    .environment(MultiAgentCartPole, env_config={"num_agents": 2})
    # Switch both the new API stack flags to True (both False by default).
    # This enables the use of
    # a) RLModule (replaces ModelV2) and Learner (replaces Policy)
    # b) and automatically picks the correct EnvRunner (single-agent vs multi-agent)
    # and enables ConnectorV2 support.
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .resources(
        num_cpus_for_main_process=1,
    )
    # We are using a simple 1-CPU setup here for learning. However, as the new stack
    # supports arbitrary scaling on the learner axis, feel free to set
    # `num_learners` to the number of available GPUs for multi-GPU training (and
    # `num_gpus_per_learner=1`).
    .learners(
        num_learners=0,  # <- in most cases, set this value to the number of GPUs
        num_gpus_per_learner=0,  # <- set this to 1, if you have at least 1 GPU
    )
    # When using RLlib's default models (RLModules) AND the new EnvRunners, you should
    # set this flag in your model config. Having to set this, will no longer be required
    # in the near future. It does yield a small performance advantage as value function
    # predictions for PPO are no longer required to happen on the sampler side (but are
    # now fully located on the learner side, which might have GPUs available).
    .training(model={"uses_new_env_runners": True})
    # Because you are in a multi-agent env, you have to set up the usual multi-agent
    # parameters:
    .multi_agent(
        policies={"p0", "p1"},
        # Map agent 0 to p0 and agent 1 to p1.
        policy_mapping_fn=lambda agent_id, episode, **kwargs: f"p{agent_id}",
    )
)

# __enabling-new-api-stack-ma-ppo-end__

# Test whether it works.
print(config.build().train())


# __enabling-new-api-stack-sa-sac-begin__

from ray.rllib.algorithms.sac import SACConfig  # noqa


config = (
    SACConfig()
    .environment("Pendulum-v1")
    # Switch both the new API stack flags to True (both False by default).
    # This enables the use of
    # a) RLModule (replaces ModelV2) and Learner (replaces Policy)
    # b) and automatically picks the correct EnvRunner (single-agent vs multi-agent)
    # and enables ConnectorV2 support.
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .resources(
        num_cpus_for_main_process=1,
    )
    # We are using a simple 1-CPU setup here for learning. However, as the new stack
    # supports arbitrary scaling on the learner axis, feel free to set
    # `num_learners` to the number of available GPUs for multi-GPU training (and
    # `num_gpus_per_learner=1`).
    .learners(
        num_learners=0,  # <- in most cases, set this value to the number of GPUs
        num_gpus_per_learner=0,  # <- set this to 1, if you have at least 1 GPU
    )
    # When using RLlib's default models (RLModules) AND the new EnvRunners, you should
    # set this flag in your model config. Having to set this, will no longer be required
    # in the near future. It does yield a small performance advantage as value function
    # predictions for PPO are no longer required to happen on the sampler side (but are
    # now fully located on the learner side, which might have GPUs available).
    .training(
        model={"uses_new_env_runners": True},
        replay_buffer_config={"type": "EpisodeReplayBuffer"},
    )
)
# __enabling-new-api-stack-sa-sac-end__


# Test whether it works.
print(config.build().train())
