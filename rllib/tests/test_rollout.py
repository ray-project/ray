from pathlib import Path
import os
import sys
import unittest


def test_rollout(algo, env="CartPole-v0"):
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
              "--stop='{\"training_iteration\": 1}'" + " --env={}".format(env))

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
    def test_a2c(self):
        test_rollout("A2C")

    def test_a3c(self):
        test_rollout("A3C")

    def test_ars(self):
        test_rollout("ARS")

    def test_ddpg(self):
        test_rollout("DDPG", env="Pendulum-v0")

    def test_dqn(self):
        test_rollout("DQN")

    def test_es(self):
        test_rollout("ES")

    def test_impala(self):
        test_rollout("IMPALA", env="Pong-ram-v4")

    def test_pg_discr(self):
        test_rollout("PG")

    def test_pg_cont(self):
        test_rollout("PG", env="Pendulum-v0")

    def test_ppo_discr(self):
        test_rollout("PPO")

    def test_ppo_cont(self):
        test_rollout("PPO", env="Pendulum-v0")

    def test_sac_discr(self):
        test_rollout("SAC")

    def test_sac_cont(self):
        test_rollout("SAC", env="Pendulum-v0")

    def test_td3(self):
        test_rollout("TD3", env="Pendulum-v0")


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
