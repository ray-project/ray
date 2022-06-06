import numpy as np
from typing import Dict, TYPE_CHECKING

from ray.rllib.agents.callbacks import DefaultCallbacks
from ray.rllib.env.base_env import BaseEnv
from ray.rllib.evaluation.episode import Episode
from ray.rllib.utils.annotations import override

if TYPE_CHECKING:
    from ray.rllib.evaluation import RolloutWorker
    from ray.rllib.policy import Policy


class RNDMetricsCallbacks(DefaultCallbacks):
    """Collects metrics for RND exploration.

    The metrics should help users to monitor the exploration of
    the environment. The metric tracked is:

    intrinsic_reward: The intrinsic reward in RND is the 
        distillation error. This error decreases over the run of 
        an experiment for already explored states. A low metric 
        indicates that the agent visits states where it has already 
        been or states that are very similar to states it visited 
        before. If the states ae truly novel this metric increases.
    """
    def __init__(self):
        super().__init__()

    def on_episode_start(
        self,
        *,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        policies: Dict[str, "Policy"],
        episode: Episode,
        env_index: int,
        **kwargs,
    ):
        assert episode.length == 0, (
            "ERROR: `on_episode_start()` callback should be called right "
            "after `env.reset()`."
        )

        episode.user_data["intrinsic_reward"] = []

    def on_episode_step(
        self,
        *,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        policies: Dict[str, "Policy"],
        episode: Episode,
        env_index: int,
        **kwargs,
    ):
        assert episode.length > 0, (
            "ERROR: `on_episode_step()` callback should not be called right "
            "after `env.reset()`."
        )

        # Get the actual state values of the NovelD exploration.
        intrinsic_reward = policies["default_policy"].get_exploration_state()

        # Average over batch.
        episode.user_data["intrinsic_reward"].append(np.mean(intrinsic_reward))

    def on_episode_end(
        self,
        *,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        policies: Dict[str, "Policy"],
        episode: Episode,
        env_index: int,
        **kwargs,
    ):
        # Average over episode.
        episode.custom_metrics["rnd/intrinsic_reward"] = np.mean(
            episode.user_data["intrinsic_reward"]
        )

        # Show also histograms of episodic intrinsic rewards.
        episode.hist_data["rnd/intrinsic_reward"] = episode.user_data[
            "intrinsic_reward"
        ]
    
class NovelDMetricsCallbacks(RNDMetricsCallbacks):
    """Collects metrics for NovelD exploration.

    The metrics should help users to monitor the exploration of
    the environment. The metrics tracked are:

    intrinsic_reward: The intrinsic reward given by NovelD for
        exploring new states. These are averaged over the
        timesteps in the episode. A high metric indicates that
        a lot of new states are explored over the course of an
        episode.
    novelty: The novelty in NovelD is the distillation error.
        This error decreases over the run of an experiment for
        already explored states. A low metric indicates that
        the agent visits states where it has already been or
        states that are very similar to states it visited before.
        If the states ae truly novel this metric increases.
    novelty_next: This is the equivalent metric for the next
        state visited (see `novelty`). Together with `novelty`
        this metric helps the user to understand the values of
        the `intrinsic_reward`.
    state_counts_total: The number of states explored over the
        course of the experiment. If this metric stagnates it is
        a sign of little exploration.
    state_counts_avg: The average number of state visits. This
        metric averages the visits to single states. If this metric
        rises it is a sign of either little exploration or of
        states that have to be crossed by the agent to go further.
        Together with `state_counts_total` this metric helps user
        to get a glimpse at state exploration. A low
        `state_counts_total` with high `state_counts_avg` is a
        strong sign of little exploration, whereas a high
        `state_counts_total` together with a low `state_counts_avg`
        is a good indicator of much exploration.
    """

    def __init__(self):
        super().__init__()

    @override(RNDMetricsCallbacks)
    def on_episode_start(
        self,
        *,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        policies: Dict[str, "Policy"],
        episode: Episode,
        env_index: int,
        **kwargs,
    ):
        assert episode.length == 0, (
            "ERROR: `on_episode_start()` callback should be called right "
            "after `env.reset()`."
        )

        super().on_episode_start(
            worker=worker,
            base_env=base_env,
            policies=policies,
            episode=episode,
            env_index=env_index,
            **kwargs,
        )
        episode.user_data["novelty"] = []
        episode.user_data["novelty_next"] = []
        episode.user_data["state_counts_total"] = []
        episode.user_data["state_counts_avg"] = []

    @override(RNDMetricsCallbacks)
    def on_episode_step(
        self,
        *,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        policies: Dict[str, "Policy"],
        episode: Episode,
        env_index: int,
        **kwargs,
    ):
        assert episode.length > 0, (
            "ERROR: `on_episode_step()` callback should not be called right "
            "after `env.reset()`."
        )

        # Get the actual state values of the NovelD exploration.
        (
            intrinsic_reward,
            novelty,
            novelty_next,
            state_counts_total,
            state_counts_avg,
        ) = policies["default_policy"].get_exploration_state()

        # Average over batch.
        episode.user_data["intrinsic_reward"].append(np.mean(intrinsic_reward))
        episode.user_data["novelty"].append(np.mean(novelty))
        episode.user_data["novelty_next"].append(np.mean(novelty_next))
        # State count metrics are scalar metrics.
        episode.user_data["state_counts_total"].append(state_counts_total)
        episode.user_data["state_counts_avg"].append(state_counts_avg)

    @override(RNDMetricsCallbacks)
    def on_episode_end(
        self,
        *,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        policies: Dict[str, "Policy"],
        episode: Episode,
        env_index: int,
        **kwargs,
    ):
        # Average over episode.
        episode.custom_metrics["noveld/intrinsic_reward"] = np.mean(
            episode.user_data["intrinsic_reward"]
        )
        episode.custom_metrics["noveld/novelty"] = np.mean(episode.user_data["novelty"])
        episode.custom_metrics["noveld/novelty_next"] = np.mean(
            episode.user_data["novelty_next"]
        )
        # Take the maximum of the total state counts to get the last
        # count of the episode.
        episode.custom_metrics["noveld/state_counts_total"] = np.max(
            episode.user_data["state_counts_total"]
        )
        episode.custom_metrics["noveld/state_counts_avg"] = np.mean(
            episode.user_data["state_counts_avg"]
        )

        # Show also histograms of episodic intrinsic rewards.
        episode.hist_data["noveld/intrinsic_reward"] = episode.user_data[
            "intrinsic_reward"
        ]
        episode.hist_data["noveld/novelty"] = episode.user_data["novelty"]
        episode.hist_data["noveld/novelty_next"] = episode.user_data["novelty_next"]
        episode.hist_data["noveld/state_counts_total"] = episode.user_data[
            "state_counts_total"
        ]
        episode.hist_data["noveld/state_counts_avg"] = episode.user_data[
            "state_counts_avg"
        ]
