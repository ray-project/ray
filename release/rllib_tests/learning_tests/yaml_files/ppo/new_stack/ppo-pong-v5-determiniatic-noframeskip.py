from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.connectors.connector_context_v2 import ConnectorContextV2
from ray.rllib.connectors.connector_pipeline_v2 import ConnectorPipelineV2, EnvToModulePipeline, ModuleToEnvPipeline

def make_sampling_connectors(env, rl_module):
    # Create the connector context to use.
    ctx = ConnectorContextV2(rl_module=rl_module, env=env)
    # Create the env-to-module connector.
    env_to_module = EnvToModulePipeline(
        ctx=ctx, connectors=[]
    )
    # Leave module-to-env undefined as we don't need any special behavior
    # here.
    # TODO (sven): Allow returning None here. Also allow returning non-pipeline
    #  individual connector. RLlib should always create pipeline automatically.
    module_to_env = ModuleToEnvPipeline(ctx=ctx)

    return env_to_module, module_to_env, ctx


def make_learner_connector(rl_module):
    # Create the connector context to use.
    ctx = ConnectorContextV2(rl_module=rl_module)
    # Create the learner connector.
    learner_connector = ConnectorPipelineV2(ctx=ctx, connectors=[])
    return learner_connector, ctx


config = (
    PPOConfig()
    .environment(
        "ALE/Pong-v5",
        env_config={
            # Make analogous to old v4 + NoFrameskip.
            "frameskip": 1,
            "full_action_space": False,
            "repeat_action_probability": 0.0,
        },
        clip_rewards=True,
    )
    .experimental(_enable_new_api_stack=True)
    .rollouts(
        sampling_connectors=make_sampling_connectors,
        env_runner_cls=SingleAgentEnvRunner,
        num_rollout_workers=59,
        num_envs_per_worker=1,
    )
    .training(
        learner_connector=make_learner_connector,
        lambda_=0.95,
        kl_coeff=0.5,
        clip_param=0.1,
        vf_clip_param=10.0,
        entropy_coeff=0.01,
        train_batch_size=16000,
        sgd_minibatch_size=2000,
        num_sgd_iter=10,
        lr=0.0001,
        grad_clip=100.0,
        grad_clip_by="global_norm",
        model={
            "vf_share_layers": True,
            "conv_filters": [[16, 4, 2], [32, 4, 2], [64, 4, 2], [128, 4, 2]],
            "conv_activation": "relu",
            "post_fcnet_hiddens": [256],
        },
    )
    .reporting(min_time_s_per_iteration=30)
    .resources(
        num_learner_workers=4,
        num_gpus_per_learner_worker=1,
        num_gpus=0,
        num_cpus_for_local_worker=1,
    )
)


if __name__ == "__main__":
    algo = config.build()
    for _ in range(1000):
        print(algo.train())
