"""Implementation of:
[1] Curiosity-driven Exploration by Self-supervised Prediction
Pathak, Agrawal, Efros, and Darrell - UC Berkeley - ICML 2017.
https://arxiv.org/pdf/1705.05363.pdf

Learns a simplified model of the environment based on three networks:
1) Embedding observations into latent space ("feature" network).
2) Predicting the action, given two consecutive embedded observations
("inverse" network).
3) Predicting the next embedded obs, given an obs and action
("forward" network).

The less the agent is able to predict the actually observed next feature
vector, given obs and action (through the forwards network), the larger the
"intrinsic reward", which will be added to the extrinsic reward.
Therefore, if a state transition was unexpected, the agent becomes
"curious" and will further explore this transition leading to better
exploration in sparse rewards environments.
"""
from collections import defaultdict

from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.connectors.env_to_module import FlattenObservations
from ray.rllib.examples.learners.classes.curiosity_ppo_torch_learner import (
    PPOConfigWithCuriosity, PPOTorchLearnerWithCuriosity
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_iters=20000,
    default_timesteps=100000000,
    default_reward=1.0,
)
parser.set_defaults(enable_new_api_stack=True)



class MeasureMaxDistanceToStart(DefaultCallbacks):
    """Callback measuring the dist of the agent to its start position in FrozenLake-v1.

    Makes the naive assumption that the start position ("S") is in the upper left
    corner of the used map.
    Uses the MetricsLogger to record the (euclidian) distance value.
    """
    def __init__(self):
        super().__init__()
        self.max_dists = defaultdict(float)
        self.max_dists_lifetime = 0.0

    def on_episode_step(
        self,
        *,
        episode,
        env_runner,
        metrics_logger,
        env,
        env_index,
        rl_module,
        **kwargs,
    ):
        obs = episode.get_observations(-1)
        num_rows = env.envs[0].unwrapped.nrow
        num_cols = env.envs[0].unwrapped.ncol
        row = obs // num_cols
        col = obs % num_rows
        curr_dist = (row ** 2 + col ** 2) ** 0.5
        if curr_dist > self.max_dists[episode.id_]:
            self.max_dists[episode.id_] = curr_dist

    def on_episode_end(
        self,
        *,
        episode,
        env_runner,
        metrics_logger,
        env,
        env_index,
        rl_module,
        **kwargs,
    ):
        # Compute current maximum distance across all running episodes
        # (including the just ended one).
        max_dist = max(self.max_dists.values())
        metrics_logger.log_value(
            key="max_dist_travelled_across_running_episodes",
            value=max_dist,
            window=10,
        )
        if max_dist > self.max_dists_lifetime:
            self.max_dists_lifetime = max_dist
        del self.max_dists[episode.id_]

    def on_sample_end(
        self,
        *,
        env_runner,
        metrics_logger,
        samples,
        **kwargs,
    ):
        metrics_logger.log_value(
            key="max_dist_travelled_lifetime",
            value=self.max_dists_lifetime,
            window=1,
        )


if __name__ == "__main__":
    args = parser.parse_args()

    base_config = (
        PPOConfigWithCuriosity()
        .environment(
            "FrozenLake-v1",
            env_config={
                # Use a 12x12 map.
                "desc": [
                    "SFFFFFFFFFFF",
                    "FFFFFFFFFFFF",
                    "FFFFFFFFFFFF",
                    "FFFFFFFFFFFF",
                    "FFFFFFFFFFFF",
                    "FFFFFFFFFFFF",
                    "FFFFFFFFFFFF",
                    "FFFFFFFFFFFF",
                    "FFFFFFFFFFFF",
                    "FFFFFFFFFFFF",
                    "FFFFFFFFFFFF",
                    "FFFFFFFFFFFG",
                ],
                "is_slippery": False,
                # Limit the number of steps the agent is allowed to make in the env to
                # make it almost impossible to learn without the curriculum.
                "max_episode_steps": 22,
            },
        )
        # Use our custom `curiosity` method to set up the ICM and our PPO/ICM-Learner.
        .curiosity(
            #curiosity_feature_net_hiddens=[256, 256],
            #curiosity_inverse_net_activation="relu"
        )
        .callbacks(MeasureMaxDistanceToStart)
        .env_runners(
            num_envs_per_env_runner=5,
            env_to_module_connector=lambda env: FlattenObservations(),
        )
        .training(
            learner_class=PPOTorchLearnerWithCuriosity,
            train_batch_size_per_learner=2000,
            num_sgd_iter=6,
            lr=0.0003,
        )
        .rl_module(model_config_dict={"vf_share_layers": True})
    )

    run_rllib_example_script_experiment(base_config, args)
