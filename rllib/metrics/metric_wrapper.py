from typing import List, Dict

from ray import tune

from rllib.metrics.metric import Metric


class MetricsWrapper:
    """ Wrapper class that gets a list of Metric implementations and
        exposes callbacks to save boilerplate

        Example:
             metrics_wrapper = MetricsWrapper([Metric1, Metric2])

             config = {
                ...
                "callbacks": metrics_wrapper.to_dict()
             }
     """

    def __init__(self, metrics: List[Metric]):
        self.metrics = metrics

    def to_dict(self):
        return {
            "on_episode_start": tune.function(self.on_episode_start),
            "on_episode_step": tune.function(self.on_episode_step),
            "on_episode_end": tune.function(self.on_episode_end),
            "on_sample_end": tune.function(self.on_sample_end),
            "on_train_result": tune.function(self.on_train_result),
            "on_postprocess_traj": tune.function(self.on_postprocess_traj)
        }

    def on_episode_start(self, info: Dict):
        for m in self.metrics:
            m.on_episode_start(info)

    def on_episode_step(self, info: Dict):
        for m in self.metrics:
            m.on_episode_step(info)

    def on_episode_end(self, info: Dict):
        for m in self.metrics:
            m.on_episode_end(info)

    def on_sample_end(self, info: Dict):
        for m in self.metrics:
            m.on_sample_end(info)

    def on_train_result(self, info: Dict):
        for m in self.metrics:
            m.on_train_result(info)

    def on_postprocess_traj(self, info: Dict):
        for m in self.metrics:
            m.on_postprocess_traj(info)
