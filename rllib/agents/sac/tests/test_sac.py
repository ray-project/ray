import unittest

import ray.rllib.agents.sac as sac
from ray.rllib.utils.framework import try_import_tf

tf = try_import_tf()


class TestSAC(unittest.TestCase):
    def test_sac_compilation(self):
        """Test whether an SACTrainer can be built with both frameworks."""
        config = sac.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        num_iterations = 2

        # eager (discrete and cont. actions).
        for fw in ["eager", "tf", "torch"]:
            if fw == "torch":
                continue
            config["eager"] = fw == "eager"
            config["use_pytorch"] = fw == "torch"
            for env in ["Pendulum-v0", "CartPole-v0"]:
                trainer = sac.SACTrainer(config=config, env=env)
                for i in range(num_iterations):
                    results = trainer.train()
                    print(results)

if __name__ == "__main__":
    import unittest
    unittest.main(verbosity=1)
