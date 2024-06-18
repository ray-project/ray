import unittest

import ray
from ray import air
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import framework_iterator
from ray import tune

torch, _ = try_import_torch()


class TestACCs(unittest.TestCase):
    def test_accs_in_non_local_mode(self):
        # Non-local mode.
        ray.init()

        actual_accs = torch.cuda.device_count()
        print(f"Actual ACCs found (by torch): {actual_accs}")

        config = PPOConfig().rollouts(num_rollout_workers=2).environment("CartPole-v1")

        # Expect errors when we run a config w/ num_accs>0 w/o a ACC
        # and _fake_accs=False.
        for num_accs in [0, 0.1, 1, actual_accs + 4]:
            # Only allow possible num_accs_per_worker (so test would not
            # block infinitely due to a down worker).
            per_worker = (
                [0] if actual_accs == 0 or actual_accs < num_accs else [0, 0.5, 1]
            )
            for num_accs_per_worker in per_worker:
                for fake_accs in [False] + ([] if num_accs == 0 else [True]):
                    config.resources(
                        num_accs=num_accs,
                        num_accs_per_worker=num_accs_per_worker,
                        _fake_accs=fake_accs,
                    )

                    print(
                        f"\n------------\nnum_accs={num_accs} "
                        f"num_accs_per_worker={num_accs_per_worker} "
                        f"_fake_accs={fake_accs}"
                    )

                    frameworks = (
                        ("tf", "torch") if num_accs > 1 else ("tf2", "tf", "torch")
                    )
                    for _ in framework_iterator(config, frameworks=frameworks):
                        # Expect that Algorithm creation causes a num_acc error.
                        if (
                            actual_accs < num_accs + 2 * num_accs_per_worker
                            and not fake_accs
                        ):
                            # "Direct" RLlib (create Algorithm on the driver).
                            # Cannot run through ray.tune.Tuner().fit() as it would
                            # simply wait infinitely for the resources to
                            # become available.
                            print("direct RLlib")
                            self.assertRaisesRegex(
                                RuntimeError,
                                "Found 0 ACCs on your machine",
                                lambda: config.build(),
                            )
                        # If actual_accs >= num_accs or faked,
                        # expect no error.
                        else:
                            print("direct RLlib")
                            algo = config.build()
                            algo.stop()
                            # Cannot run through ray.tune.Tuner().fit() w/ fake ACCs
                            # as it would simply wait infinitely for the
                            # resources to become available (even though, we
                            # wouldn't really need them).
                            if num_accs == 0:
                                print("via ray.tune.Tuner().fit()")
                                tune.Tuner(
                                    "PPO",
                                    param_space=config,
                                    run_config=air.RunConfig(
                                        stop={"training_iteration": 0}
                                    ),
                                ).fit()
        ray.shutdown()

    def test_accs_in_local_mode(self):
        # Local mode.
        ray.init(local_mode=True)

        actual_accs_available = torch.cuda.device_count()

        config = PPOConfig().rollouts(num_rollout_workers=2).environment("CartPole-v1")

        # Expect no errors in local mode.
        for num_accs in [0, 0.1, 1, actual_accs_available + 4]:
            print(f"num_accs={num_accs}")
            for fake_accs in [False, True]:
                print(f"_fake_accs={fake_accs}")
                config.resources(num_accs=num_accs, _fake_accs=fake_accs)
                frameworks = ("tf", "torch") if num_accs > 1 else ("tf2", "tf", "torch")
                for _ in framework_iterator(config, frameworks=frameworks):
                    print("direct RLlib")
                    algo = config.build()
                    algo.stop()
                    print("via ray.tune.Tuner().fit()")
                    tune.Tuner(
                        "PPO",
                        param_space=config,
                        run_config=air.RunConfig(stop={"training_iteration": 0}),
                    ).fit()

        ray.shutdown()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
