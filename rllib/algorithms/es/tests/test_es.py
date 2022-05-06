import unittest

import ray
import ray.rllib.algorithms.es as es
from ray.rllib.utils.test_utils import check_compute_single_action, framework_iterator


class TestES(unittest.TestCase):
    def test_es_compilation(self):
        """Test whether an ESTrainer can be built on all frameworks."""
        ray.init(num_cpus=4)
        config = es.ESConfig()
        # Keep it simple.
        config.training(
            model={
                "fcnet_hiddens": [10],
                "fcnet_activation": None,
            },
            noise_size=2500000,
            episodes_per_batch=10,
            train_batch_size=100,
        )
        config.rollouts(num_rollout_workers=1)
        # Test eval workers ("normal" WorkerSet, unlike ES' list of
        # RolloutWorkers used for collecting train batches).
        config.evaluation(evaluation_interval=1, evaluation_num_workers=2)

        num_iterations = 1

        for _ in framework_iterator(config):
            for env in ["CartPole-v0", "Pendulum-v1"]:
                trainer = config.build(env=env)
                for i in range(num_iterations):
                    results = trainer.train()
                    print(results)

                check_compute_single_action(trainer)
                trainer.stop()
        ray.shutdown()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
