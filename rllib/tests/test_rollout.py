# Simple translation of former test_rollout.sh file to be able
# to run this in bazel test suite.

from pathlib import Path
import os
import unittest


class TestRollout(unittest.TestCase):
    def test_rollout(self):
        tmp_dir = os.popen("mktemp -d").read()[:-1]
        if not os.path.exists(tmp_dir):
            sys.exit(1)

        print("Saving results to {}".format(tmp_dir))

        rllib_dir = str(Path(__file__).parent.parent.absolute())
        print("RLlib dir = {}\nexists={}".format(rllib_dir,
                                                 os.path.exists(rllib_dir)))
        os.system("python {}/train.py --local-dir={} --run=IMPALA "
                  "--checkpoint-freq=1 ".format(rllib_dir, tmp_dir) +
                  "--config='{\"num_workers\": 1, \"num_gpus\": 0}' "
                  "--env=Pong-ram-v4 --stop='{\"training_iteration\": 1}'")

        checkpoint_path = os.popen(
            "ls {}/default/*/checkpoint_1/checkpoint-1".format(
                tmp_dir)).read()[:-1]
        print("Checkpoint path {}".format(checkpoint_path))
        if not os.path.exists(checkpoint_path):
            sys.exit(1)

        os.popen("python {}/rollout.py --run=IMPALA \"{}\" --steps=100 "
                 "--out=\"{}/rollouts_100steps.pkl\" --no-render".format(
                     rllib_dir, checkpoint_path, tmp_dir)).read()
        if not os.path.exists(tmp_dir + "/rollouts_100steps.pkl"):
            sys.exit(1)

        os.popen("python {}/rollout.py --run=IMPALA \"{}\" --episodes=1 "
                 "--out=\"{}/rollouts_1episode.pkl\" --no-render".format(
                     rllib_dir, checkpoint_path, tmp_dir)).read()
        if not os.path.exists(tmp_dir + "/rollouts_1episode.pkl"):
            sys.exit(1)

        # Cleanup.
        os.popen("rm -rf \"{}\"".format(tmp_dir)).read()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
