import os
from pathlib import Path
import re
import sys
import unittest

import ray
from ray import air
from ray import tune
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.utils.test_utils import framework_iterator


def evaluate_test(algo, env="CartPole-v0", test_episode_rollout=False):
    extra_config = ""
    if algo == "ARS":
        extra_config = ',"train_batch_size": 10, "noise_size": 250000'
    elif algo == "ES":
        extra_config = (
            ',"episodes_per_batch": 1,"train_batch_size": 10, ' '"noise_size": 250000'
        )

    for fw in framework_iterator(frameworks=("tf", "torch")):
        fw_ = ', "framework": "{}"'.format(fw)

        tmp_dir = os.popen("mktemp -d").read()[:-1]
        if not os.path.exists(tmp_dir):
            sys.exit(1)

        print("Saving results to {}".format(tmp_dir))

        rllib_dir = str(Path(__file__).parent.parent.absolute())
        print("RLlib dir = {}\nexists={}".format(rllib_dir, os.path.exists(rllib_dir)))
        os.system(
            "python {}/train.py --local-dir={} --run={} "
            "--checkpoint-freq=1 ".format(rllib_dir, tmp_dir, algo)
            + "--config='{"
            + '"num_workers": 1, "num_gpus": 0{}{}'.format(fw_, extra_config)
            + ', "min_sample_timesteps_per_iteration": 5,'
            '"min_time_s_per_iteration": 0.1, '
            '"model": {"fcnet_hiddens": [10]}'
            "}' --stop='{\"training_iteration\": 1}'" + " --env={}".format(env)
        )

        checkpoint_path = os.popen(
            "ls {}/default/*/checkpoint_000001/algorithm_state.pkl".format(tmp_dir)
        ).read()[:-1]
        if not os.path.exists(checkpoint_path):
            sys.exit(1)
        print("Checkpoint path {} (exists)".format(checkpoint_path))

        # Test rolling out n steps.
        os.popen(
            'python {}/evaluate.py --run={} "{}" --steps=10 '
            '--out="{}/rollouts_10steps.pkl"'.format(
                rllib_dir, algo, checkpoint_path, tmp_dir
            )
        ).read()
        if not os.path.exists(tmp_dir + "/rollouts_10steps.pkl"):
            sys.exit(1)
        print("evaluate output (10 steps) exists!")

        # Test rolling out 1 episode.
        if test_episode_rollout:
            os.popen(
                'python {}/evaluate.py --run={} "{}" --episodes=1 '
                '--out="{}/rollouts_1episode.pkl"'.format(
                    rllib_dir, algo, checkpoint_path, tmp_dir
                )
            ).read()
            if not os.path.exists(tmp_dir + "/rollouts_1episode.pkl"):
                sys.exit(1)
            print("evaluate output (1 ep) exists!")

        # Cleanup.
        os.popen('rm -rf "{}"'.format(tmp_dir)).read()


def learn_test_plus_evaluate(algo, env="CartPole-v0"):
    for fw in framework_iterator(frameworks=("tf", "torch")):
        fw_ = ', \\"framework\\": \\"{}\\"'.format(fw)

        tmp_dir = os.popen("mktemp -d").read()[:-1]
        if not os.path.exists(tmp_dir):
            # Last resort: Resolve via underlying tempdir (and cut tmp_.
            tmp_dir = ray._private.utils.tempfile.gettempdir() + tmp_dir[4:]
            if not os.path.exists(tmp_dir):
                sys.exit(1)

        print("Saving results to {}".format(tmp_dir))

        rllib_dir = str(Path(__file__).parent.parent.absolute())
        print("RLlib dir = {}\nexists={}".format(rllib_dir, os.path.exists(rllib_dir)))
        os.system(
            "python {}/train.py --local-dir={} --run={} "
            "--checkpoint-freq=1 --checkpoint-at-end ".format(rllib_dir, tmp_dir, algo)
            + '--config="{\\"num_gpus\\": 0, \\"num_workers\\": 1, '
            '\\"evaluation_config\\": {\\"explore\\": false}'
            + fw_
            + '}" '
            + '--stop="{\\"episode_reward_mean\\": 100.0}"'
            + " --env={}".format(env)
        )

        # Find last checkpoint and use that for the rollout.
        checkpoint_path = os.popen(
            "ls {}/default/*/checkpoint_*/algorithm_state.pkl".format(tmp_dir)
        ).read()[:-1]
        checkpoints = [
            cp
            for cp in checkpoint_path.split("\n")
            if re.match(r"^.+algorithm_state.pkl$", cp)
        ]
        # Sort by number and pick last (which should be the best checkpoint).
        last_checkpoint = sorted(
            checkpoints,
            key=lambda x: int(re.match(r".+checkpoint_(\d+).+", x).group(1)),
        )[-1]
        assert re.match(r"^.+checkpoint_\d+/algorithm_state.pkl$", last_checkpoint)
        if not os.path.exists(last_checkpoint):
            sys.exit(1)
        print("Best checkpoint={} (exists)".format(last_checkpoint))

        # Test rolling out n steps.
        result = os.popen(
            "python {}/evaluate.py --run={} "
            "--steps=400 "
            '--out="{}/rollouts_n_steps.pkl" "{}"'.format(
                rllib_dir, algo, tmp_dir, last_checkpoint
            )
        ).read()[:-1]
        if not os.path.exists(tmp_dir + "/rollouts_n_steps.pkl"):
            sys.exit(1)
        print("Rollout output exists -> Checking reward ...")
        episodes = result.split("\n")
        mean_reward = 0.0
        num_episodes = 0
        for ep in episodes:
            mo = re.match(r"Episode .+reward: ([\d\.\-]+)", ep)
            if mo:
                mean_reward += float(mo.group(1))
                num_episodes += 1
        mean_reward /= num_episodes
        print("Rollout's mean episode reward={}".format(mean_reward))
        assert mean_reward >= 100.0

        # Cleanup.
        os.popen('rm -rf "{}"'.format(tmp_dir)).read()


def learn_test_multi_agent_plus_evaluate(algo):
    for fw in framework_iterator(frameworks=("tf", "torch")):
        tmp_dir = os.popen("mktemp -d").read()[:-1]
        if not os.path.exists(tmp_dir):
            # Last resort: Resolve via underlying tempdir (and cut tmp_.
            tmp_dir = ray._private.utils.tempfile.gettempdir() + tmp_dir[4:]
            if not os.path.exists(tmp_dir):
                sys.exit(1)

        print("Saving results to {}".format(tmp_dir))

        rllib_dir = str(Path(__file__).parent.parent.absolute())
        print("RLlib dir = {}\nexists={}".format(rllib_dir, os.path.exists(rllib_dir)))

        def policy_fn(agent_id, episode, **kwargs):
            return "pol{}".format(agent_id)

        config = {
            "num_gpus": 0,
            "num_workers": 1,
            "evaluation_config": {"explore": False},
            "framework": fw,
            "env": MultiAgentCartPole,
            "multiagent": {
                "policies": {"pol0", "pol1"},
                "policy_mapping_fn": policy_fn,
            },
        }
        stop = {"episode_reward_mean": 100.0}
        results = tune.Tuner(
            algo,
            param_space=config,
            run_config=air.RunConfig(
                stop=stop,
                verbose=1,
                checkpoint_config=air.CheckpointConfig(
                    checkpoint_frequency=1, checkpoint_at_end=True
                ),
                local_dir=tmp_dir,
            ),
        ).fit()

        # Find last checkpoint and use that for the rollout.
        best_checkpoint = results.get_best_result(
            metric="episode_reward_mean",
            mode="max",
        ).checkpoint

        ray.shutdown()

        # Test rolling out n steps.
        result = os.popen(
            "python {}/evaluate.py --run={} "
            "--steps=400 "
            '--out="{}/rollouts_n_steps.pkl" "{}"'.format(
                rllib_dir, algo, tmp_dir, best_checkpoint._local_path
            )
        ).read()[:-1]
        if not os.path.exists(tmp_dir + "/rollouts_n_steps.pkl"):
            sys.exit(1)
        print("Rollout output exists -> Checking reward ...")
        episodes = result.split("\n")
        mean_reward = 0.0
        num_episodes = 0
        for ep in episodes:
            mo = re.match(r"Episode .+reward: ([\d\.\-]+)", ep)
            if mo:
                mean_reward += float(mo.group(1))
                num_episodes += 1
        mean_reward /= num_episodes
        print("Rollout's mean episode reward={}".format(mean_reward))
        assert mean_reward >= 100.0

        # Cleanup.
        os.popen('rm -rf "{}"'.format(tmp_dir)).read()


class TestEvaluate1(unittest.TestCase):
    def test_a3c(self):
        evaluate_test("A3C")

    def test_ddpg(self):
        evaluate_test("DDPG", env="Pendulum-v1")


class TestEvaluate2(unittest.TestCase):
    def test_dqn(self):
        evaluate_test("DQN")

    def test_es(self):
        evaluate_test("ES")


class TestEvaluate3(unittest.TestCase):
    def test_impala(self):
        evaluate_test("IMPALA", env="CartPole-v0")

    def test_ppo(self):
        evaluate_test("PPO", env="CartPole-v0", test_episode_rollout=True)


class TestEvaluate4(unittest.TestCase):
    def test_sac(self):
        evaluate_test("SAC", env="Pendulum-v1")


class TestTrainAndEvaluate(unittest.TestCase):
    def test_ppo_train_then_rollout(self):
        learn_test_plus_evaluate("PPO")

    def test_ppo_multi_agent_train_then_rollout(self):
        learn_test_multi_agent_plus_evaluate("PPO")


if __name__ == "__main__":
    import pytest

    # One can specify the specific TestCase class to run.
    # None for all unittest.TestCase classes in this file.
    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(["-v", __file__ + ("" if class_ is None else "::" + class_)]))
