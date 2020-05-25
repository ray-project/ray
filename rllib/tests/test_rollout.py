from pathlib import Path
import os
import sys
import unittest


def rollout_test(algo, env="CartPole-v0"):
    tmp_dir = os.popen("mktemp -d").read()[:-1]
    if not os.path.exists(tmp_dir):
        sys.exit(1)

    print("Saving results to {}".format(tmp_dir))

    rllib_dir = str(Path(__file__).parent.parent.absolute())
    print("RLlib dir = {}\nexists={}".format(rllib_dir,
                                             os.path.exists(rllib_dir)))
    os.system("python {}/train.py --local-dir={} --run={} "
              "--checkpoint-freq=1 ".format(rllib_dir, tmp_dir, algo) +
              "--config='{\"num_workers\": 1, \"num_gpus\": 0}' "
              "--stop='{\"training_iteration\": 1, "
              "\"timesteps_per_iter\": 10, "
              "\"min_iter_time_s\": 1}'" + " --env={}".format(env))

    checkpoint_path = os.popen(
        "ls {}/default/*/checkpoint_1/checkpoint-1".format(tmp_dir)).read()[:
                                                                            -1]
    if not os.path.exists(checkpoint_path):
        sys.exit(1)
    print("Checkpoint path {} (exists)".format(checkpoint_path))

    # Test rolling out n steps.
    os.popen("python {}/rollout.py --run={} \"{}\" --steps=25 "
             "--out=\"{}/rollouts_25steps.pkl\" --no-render".format(
                 rllib_dir, algo, checkpoint_path, tmp_dir)).read()
    if not os.path.exists(tmp_dir + "/rollouts_25steps.pkl"):
        sys.exit(1)
    print("rollout output (25 steps) exists!".format(checkpoint_path))

    # Test rolling out 1 episode.
    os.popen("python {}/rollout.py --run={} \"{}\" --episodes=1 "
             "--out=\"{}/rollouts_1episode.pkl\" --no-render".format(
                 rllib_dir, algo, checkpoint_path, tmp_dir)).read()
    if not os.path.exists(tmp_dir + "/rollouts_1episode.pkl"):
        sys.exit(1)
    print("rollout output (1 ep) exists!".format(checkpoint_path))

    # Cleanup.
    os.popen("rm -rf \"{}\"".format(tmp_dir)).read()


class TestRollout(unittest.TestCase):
    def test_a3c(self):
        rollout_test("A3C")

    def test_ars(self):
        rollout_test("ARS")

    def test_ddpg(self):
        rollout_test("DDPG", env="Pendulum-v0")

    def test_dqn(self):
        rollout_test("DQN")

    def test_es(self):
        rollout_test("ES")

    def test_impala(self):
        rollout_test("IMPALA", env="Pong-ram-v4")

    def test_pg(self):
        rollout_test("PG")

    def test_ppo(self):
        rollout_test("PPO", env="Pendulum-v0")

    def test_sac(self):
        rollout_test("SAC", env="Pendulum-v0")


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
