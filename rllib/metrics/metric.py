from abc import abstractmethod
from typing import Dict


class Metric:
    """ Abstact class for metric callbacks. Please see MetricWrapper
    for more details """

    @staticmethod
    @abstractmethod
    def on_episode_start(info: Dict) -> None:
        """
        Runs before an episode starts. This is where buffer initialization and
        counter resetting is being done.info["episode"].user_data is the
        natural place to store intermediate data
        :param info: dictionary containing the following keys:
            "env": ray.rllib.BaseEnv instance running the episodes
            "policy": Dict[str, Policy] - mapping all available policies
            "episode": MultiAgentEpisode - see class
        """
        pass

    @staticmethod
    @abstractmethod
    def on_episode_step(info: Dict) -> None:
        """
        Runs with every decision step. This is naturally where
        info["episode"].user_data would be updated.
        :param info: dictionary containing the following keys:
            "env": ray.rllib.BaseEnv instance running the episodes
            "episode": MultiAgentEpisode - see class
        """
        pass

    @staticmethod
    @abstractmethod
    def on_episode_end(info: Dict) -> None:
        """
        Runs when an episode is done. This is naturally where
        info["episode"].user_data would be aggregated into a
        readable metric.
        :param info: dictionary containing the following keys:
            "env": ray.rllib.BaseEnv instance running the episodes
            "policy": Dict[str, Policy] - mapping all available policies
            "episode": MultiAgentEpisode - see class
        """
        pass

    @staticmethod
    @abstractmethod
    def on_sample_end(info: Dict) -> None:
        """
        Callback that is called at the end of sampling a new SampleBatch
        in RolloutWorker.sample()
        :param info: dictionary containing the following keys:
            "worker": the RolloutWorker generated the sample
            "samples": SampleBatch of samples drawn from an environment
        """
        pass

    @staticmethod
    @abstractmethod
    def on_train_result(info: Dict) -> None:
        """
        Runs at the end of Trainable.train() call, i.e. after each
        policy update.
        :param info: dictionary containing the following keys:
            "trainer": Trainer class
            "result": dictionary with all metric summaries
        """
        pass

    @staticmethod
    @abstractmethod
    def on_postprocess_traj(info: Dict) -> None:
        """
        This callback is being called after a Policy object's
        postprocess_fn has being called. This is where an aggregation
        of results of postprocess_fn artifacts would be implemented
        :param info: dictionary containing the following keys:
            "episode": MultiAgentEpisode - see class
            "agent_id": str
            "pre_batch": Tuple[Policy, SampleBatch] (preprocessed batch)
            "post_batch: SampleBatch - postprocessed batch
            "all_pre_batches": Dict[str, Tuple[Policy, SampleBatch]] -
                mapping all agent_ids to their pre_batch
        """
        pass
