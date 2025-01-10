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
        if config.num_learners > 0:
            if config.num_gpus_per_learner:
                # to prevent learner gpus from having their corresponding CPUs
                # being scheduled for non-learner actors, we make GPU only
                # bundles which will be used by the learner workers.
                learner_bundles = [
                    {"GPU": config.num_gpus_per_learner}
                    for _ in range(config.num_learners)
                ]
            elif config.num_cpus_per_learner:
                learner_bundles = [
                    {"CPU": config.num_cpus_per_learner}
                    for _ in range(config.num_learners)
                ]
        else:
            learner_bundles = [
                {
                    # sampling and training is not done concurrently when local is
                    # used, so pick the max.
                    "CPU": max(
                        config.num_cpus_per_learner,
                        config.num_cpus_for_main_process,
                    ),
                    "GPU": config.num_gpus_per_learner,
                }
            ]
        return learner_bundles
