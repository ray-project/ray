import numpy as np

from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.tune.registry import register_env


register_env("multi_cartpole", lambda _: MultiAgentCartPole({"num_agents": 2}))

# Number of policies overall in the PolicyMap.
num_policies = 1000
# Number of thos policies that should be trained. These are a subset of `num_policies`.
num_trainable = 50

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
        policies_swappable=True,
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
)

# Define some stopping criteria.
stop = dict({
    # Any of the learning policies may reach the reward in order for this test
    # to succeed (to speed things up a little; some trainable policies may receive
    # more or less data and may thus learn more or less quickly).
    f"policy_reward_mean/pol{i}": 50.0 for i in range(num_trainable)
}, **{"timesteps_total": 400000})
