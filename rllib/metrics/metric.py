from abc import abstractmethod
from typing import Dict


class Metric:
    @staticmethod
    @abstractmethod
    def on_episode_start(info: Dict) -> None:
        pass

    @staticmethod
    @abstractmethod
    def on_episode_step(info: Dict) -> None:
        pass

    @staticmethod
    @abstractmethod
    def on_episode_end(info: Dict) -> None:
        pass

    @staticmethod
    @abstractmethod
    def on_sample_end(info: Dict) -> None:
        pass

    @staticmethod
    @abstractmethod
    def on_train_result(info: Dict) -> None:
        pass

    @staticmethod
    @abstractmethod
    def on_postprocess_traj(info: Dict) -> None:
        pass
