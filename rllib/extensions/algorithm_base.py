from typing import Dict, List

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig


class AlgorithmBase:
    @staticmethod
    def _get_learner_bundles(config: AlgorithmConfig) -> List[Dict[str, int]]:
        """Selects the right resource bundles for learner workers based off of config.

        Args:
            config: The algorithm config.

        Returns:
            A list of resource bundles for the learner workers.
        """
        # TODO (sven): Add this assert, once rayturbo uses the latest master,
        #  which does not call this method anymore when `num_learners=0`.
        # assert config.num_learners == 0
        _num_agg = getattr(config, "num_aggregator_actors_per_learner", 0)

        if config.num_learners > 0:
            per_learner = {
                "CPU": (
                    config.num_cpus_per_learner
                    if config.num_gpus_per_learner == 0
                    else 0
                )
                + config.num_aggregator_actors_per_learner,
                "GPU": max(0, config.num_gpus_per_learner),
            }

            return [per_learner] * config.num_learners

        # TODO (sven): Remove this logic, once rayturbo uses the latest master,
        #  which does not call this method anymore when `num_learners=0`.
        else:
            learner_bundles = [
                {
                    # sampling and training is not done concurrently when local is
                    # used, so pick the max.
                    "CPU": max(
                        config.num_cpus_per_learner,
                        config.num_cpus_for_main_process,
                    )
                    + config.num_aggregator_actors_per_learner,
                    "GPU": config.num_gpus_per_learner,
                }
            ]
            return learner_bundles
