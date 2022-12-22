import numpy as np

from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.tune.registry import register_env


register_env("multi_cartpole", lambda _: MultiAgentCartPole({"num_agents": 2}))

# Number of policies overall in the PolicyMap.
num_policies = 100
# Number of those policies that should be trained. These are a subset of `num_policies`.
num_trainable = 20

num_envs_per_worker = 5

# Define the config as an APPOConfig object.
config = (
    APPOConfig()
    .environment("multi_cartpole")
    .rollouts(
        num_rollout_workers=4,
        num_envs_per_worker=num_envs_per_worker,
        observation_filter="MeanStdFilter",
    )
    .training(
        model={
            "fcnet_hiddens": [32],
            "fcnet_activation": "linear",
            "vf_share_layers": True,
        },
        num_sgd_iter=1,
        vf_loss_coeff=0.005,
        vtrace=True,
        vtrace_drop_last_ts=False,
    )
    .multi_agent(
        # 2 agents per sub-env.
        # This is to avoid excessive swapping during an episode rollout, since
        # Policies are only re-picked at the beginning of each episode.
        policy_map_capacity=2 * num_envs_per_worker,
        policy_states_are_swappable=True,
        policies={f"pol{i}" for i in range(num_policies)},
        # Train only the first n policies.
        policies_to_train=[f"pol{i}" for i in range(num_trainable)],
        # Pick one trainable and one non-trainable policy per episode.
        policy_mapping_fn=(
            lambda aid, eps, worker, **kw: "pol"
            + str(
                np.random.randint(0, num_trainable)
                if aid == 0
                else np.random.randint(num_trainable, num_policies)
            )
        ),
    )
    # On the eval track, always let policy 0 play so we get its results in each results
    # dict.
    .evaluation(
        evaluation_config=APPOConfig.overrides(
            policy_mapping_fn=(
                lambda aid, eps, worker, **kw: "pol"
                + str(0 if aid == 0 else np.random.randint(num_trainable, num_policies))
            ),
        ),
        evaluation_num_workers=2,
        evaluation_interval=1,
        evaluation_parallel_to_training=True,
    )
)

# Define some stopping criteria.
stop = {
    "evaluation/policy_reward_mean/pol0": 50.0,
    "timesteps_total": 500000,
}
