# @OldAPIStack
from ray.rllib.algorithms.impala import IMPALAConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray import tune

tune.registry.register_env("env", lambda cfg: MultiAgentCartPole(config=cfg))


config = (
    IMPALAConfig()
    .environment("env", env_config={"num_agents": 4})
    .env_runners(
        num_envs_per_env_runner=5,
        num_env_runners=4,
        observation_filter="MeanStdFilter",
    )
    .resources(num_gpus=1, _fake_gpus=True)
    .multi_agent(
        policies=["p0", "p1", "p2", "p3"],
        policy_mapping_fn=(lambda agent_id, episode, worker, **kwargs: f"p{agent_id}"),
    )
    .training(
        num_epochs=1,
        vf_loss_coeff=0.005,
        vtrace=True,
        model={
            "fcnet_hiddens": [32],
            "fcnet_activation": "linear",
            "vf_share_layers": True,
        },
        replay_proportion=0.0,
    )
)

stop = {
    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 600,  # 600 / 4 (==num_agents) = 150
    f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000,
}
