from typing import Dict

import ray
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger


@ray.remote
class BatchDispatcher:
    def __init__(self):

        self._learners = []
        self._batch_refs = []
        self._total_timesteps = 0
        self._learner_idx = 0

    def sync(self):
        return None

    def set_other_actors(self, *, metrics_actor, learners):
        self._metrics_actor = metrics_actor
        self._learners = learners

    def add_batch(self, batch_ref):
        assert isinstance(batch_ref["batch"], ray.ObjectRef)
        self._batch_refs.append(batch_ref["batch"])

        while len(self._batch_refs) >= len(self._learners):
            for idx, learner in enumerate(self._learners):
                learner.update.remote(
                    self._batch_refs.pop(0),
                    self._total_timesteps,
                    send_weights=(idx == self._learner_idx),
                )
            self._learner_idx += 1
            self._learner_idx %= len(self._learners)


@ray.remote
class MetricsActor:
    def __init__(self):
        self.metrics = MetricsLogger()

    def add(
        self,
        *,
        env_runner_metrics=None,
        aggregator_metrics=None,
        learner_metrics=None,
    ):
        if env_runner_metrics is not None:
            assert isinstance(env_runner_metrics, dict)
            self.metrics.merge_and_log_n_dicts(
                [env_runner_metrics],
                key="env_runners",
            )
        if aggregator_metrics is not None:
            assert isinstance(aggregator_metrics, dict)
            self.metrics.merge_and_log_n_dicts(
                [aggregator_metrics],
                key="aggregator_actors",
            )
        if learner_metrics is not None:
            assert isinstance(learner_metrics, dict)
            self.metrics.merge_and_log_n_dicts(
                [learner_metrics],
                key="learners",
            )

    def get(self):
        metrics = self.metrics.reduce()
        return metrics



@ray.remote
class WeightsServerActor:
    """A simple weights (reference) server actor to distribute model weights.

    Call `put()` to store new weights (and have them broadcast to other peer actors.
    Call `get()` to retreive the latest weights (as a ray ref).
    """
    def __init__(self):
        """Initializes a WeightsServerActor instance."""
        self._weights_ref = None
        self._other_weights_server_actors = []

    def add_peers(self, other_weights_server_actors):
        """Defines the peer actors of this one."""
        self._other_weights_server_actors = other_weights_server_actors

    def put(self, weights_ref: Dict[str, ray.ObjectRef], broadcast: bool = False):
        # Store new weights reference.

        self._weights_ref = weights_ref
        # Send new weights to all peers (but tell each peer to NOT broadcast it to all
        # their peers as this would cause an endless broadcasting loop).
        if broadcast:
            for peer in self._other_weights_server_actors:
                peer.put.remote(weights_ref, broadcast=False)

    def get(self) -> Dict[str, ray.ObjectRef]:
        """Returns the current weights ray reference."""
        return self._weights_ref


