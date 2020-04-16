import unittest

import ray
import ray.rllib.agents.es as es
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import framework_iterator

tf = try_import_tf()


class TestES(unittest.TestCase):
    def test_es_compilation(self):
        """Test whether an ESTrainer can be built on all frameworks."""
        ray.init()
        config = es.DEFAULT_CONFIG.copy()
        num_iterations = 2

        for _ in framework_iterator(config, ("tf", )):
            plain_config = config.copy()
            trainer = es.ESTrainer(config=plain_config, env="CartPole-v0")
            for i in range(num_iterations):
                results = trainer.train()
                print(results)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
