# __enabling-new-api-stack-sa-ppo-begin__

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner


config = (
    PPOConfig().environment("CartPole-v1")
    # Switch the new API stack flag to True (False by default).
    # This enables the use of the RLModule (replaces ModelV2) AND Learner (replaces
    # Policy) classes.
    .experimental(_enable_new_api_stack=True)
    # However, the above flag only activates the RLModule and Learner APIs. In order
    # to utilize all of the new API stack's classes, you also have to specify the
    # EnvRunner (replaces RolloutWorker) to use.
    # Note that this step will be fully automated in the next release.
    # Set the `env_runner_cls` to `SingleAgentEnvRunner` for single-agent setups and
    # `MultiAgentEnvRunner` for multi-agent cases.
    .env_runners(env_runner_cls=SingleAgentEnvRunner)
    # We are using a simple 1-CPU setup here for learning. However, as the new stack
    # supports arbitrary scaling on the learner axis, feel free to set
    # `num_learner_workers` to the number of available GPUs for multi-GPU training (and
    # `num_gpus_per_learner_worker=1`).
    .resources(
        num_learner_workers=0,  # <- in most cases, set this value to the number of GPUs
        num_gpus_per_learner_worker=0,  # <- set this to 1, if you have at least 1 GPU
        num_cpus_for_local_worker=1,
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
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner  # noqa
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole  # noqa


# A typical multi-agent setup (otherwise using the exact same parameters as before)
# looks like this.
config = (
    PPOConfig().environment(MultiAgentCartPole, env_config={"num_agents": 2})
    # Switch the new API stack flag to True (False by default).
    # This enables the use of the RLModule (replaces ModelV2) AND Learner (replaces
    # Policy) classes.
    .experimental(_enable_new_api_stack=True)
    # However, the above flag only activates the RLModule and Learner APIs. In order
    # to utilize all of the new API stack's classes, you also have to specify the
    # EnvRunner (replaces RolloutWorker) to use.
    # Note that this step will be fully automated in the next release.
    # Set the `env_runner_cls` to `SingleAgentEnvRunner` for single-agent setups and
    # `MultiAgentEnvRunner` for multi-agent cases.
    .env_runners(env_runner_cls=MultiAgentEnvRunner)
    # We are using a simple 1-CPU setup here for learning. However, as the new stack
    # supports arbitrary scaling on the learner axis, feel free to set
    # `num_learner_workers` to the number of available GPUs for multi-GPU training (and
    # `num_gpus_per_learner_worker=1`).
    .resources(
        num_learner_workers=0,  # <- in most cases, set this value to the number of GPUs
        num_gpus_per_learner_worker=0,  # <- set this to 1, if you have at least 1 GPU
        num_cpus_for_local_worker=1,
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
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner  # noqa


config = (
    SACConfig().environment("Pendulum-v1")
    # Switch the new API stack flag to True (False by default).
    # This enables the use of the RLModule (replaces ModelV2) AND Learner (replaces
    # Policy) classes.
    .experimental(_enable_new_api_stack=True)
    # However, the above flag only activates the RLModule and Learner APIs. In order
    # to utilize all of the new API stack's classes, you also have to specify the
    # EnvRunner (replaces RolloutWorker) to use.
    # Note that this step will be fully automated in the next release.
    .env_runners(env_runner_cls=SingleAgentEnvRunner)
    # We are using a simple 1-CPU setup here for learning. However, as the new stack
    # supports arbitrary scaling on the learner axis, feel free to set
    # `num_learner_workers` to the number of available GPUs for multi-GPU training (and
    # `num_gpus_per_learner_worker=1`).
    .resources(
        num_learner_workers=0,  # <- in most cases, set this value to the number of GPUs
        num_gpus_per_learner_worker=0,  # <- set this to 1, if you have at least 1 GPU
        num_cpus_for_local_worker=1,
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
