import os
from pathlib import Path
import re
import sys
import unittest

import ray
from ray import air, tune
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.utils.test_utils import framework_iterator
from ray.tune.registry import get_trainable_cls

# The new RLModule / Learner API
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec

rllib_dir = str(Path(__file__).parent.parent.absolute())


def evaluate_test(algo, env="CartPole-v1", test_episode_rollout=False):
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


def learn_test_plus_evaluate(algo: str, env="CartPole-v1"):
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

        # This is only supported without RLModule API. See AlgorithmConfig for
        # more info. We need to prefetch the default config that will be used when we
        # call rllib train here to see if the RLModule API is enabled.
        algo_cls = get_trainable_cls(algo)
        config = algo_cls.get_default_config()
        if config._enable_rl_module_api:
            eval_ = ', \\"evaluation_config\\": {}'
        else:
            eval_ = ', \\"evaluation_config\\": {\\"explore\\": false}'

        print("RLlib dir = {}\nexists={}".format(rllib_dir, os.path.exists(rllib_dir)))
        os.system(
            "python {}/train.py --local-dir={} --run={} "
            "--checkpoint-freq=1 --checkpoint-at-end ".format(rllib_dir, tmp_dir, algo)
            + '--config="{\\"num_gpus\\": 0, \\"num_workers\\": 1'
            + eval_
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


def learn_test_multi_agent_plus_evaluate(algo: str):
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

        config = (
            get_trainable_cls(algo)
            .get_default_config()
            .environment(MultiAgentCartPole)
            .framework(fw)
            .rollouts(num_rollout_workers=1)
            .multi_agent(
                policies={"pol0", "pol1"},
                policy_mapping_fn=policy_fn,
            )
            .resources(num_gpus=0)
            .evaluation(evaluation_config=AlgorithmConfig.overrides(explore=True))
            .evaluation(evaluation_config=AlgorithmConfig.overrides(explore=True))
            .rl_module(
                rl_module_spec=MultiAgentRLModuleSpec(
                    module_specs={
                        "pol0": SingleAgentRLModuleSpec(),
                        "pol1": SingleAgentRLModuleSpec(),
                    }
                ),
            )
        )

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
                failure_config=air.FailureConfig(fail_fast="raise"),
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
        evaluate_test("IMPALA", env="CartPole-v1")

    def test_ppo(self):
        evaluate_test("PPO", env="CartPole-v1", test_episode_rollout=True)


class TestEvaluate4(unittest.TestCase):
    def test_sac(self):
        evaluate_test("SAC", env="Pendulum-v1")


class TestTrainAndEvaluate(unittest.TestCase):
    def test_ppo_train_then_rollout(self):
        learn_test_plus_evaluate("PPO")

    def test_ppo_multi_agent_train_then_rollout(self):
        learn_test_multi_agent_plus_evaluate("PPO")


class TestCLISmokeTests(unittest.TestCase):
    def test_help(self):
        assert os.popen(f"python {rllib_dir}/scripts.py --help").read()
        assert os.popen(f"python {rllib_dir}/train.py --help").read()
        assert os.popen(f"python {rllib_dir}/train.py file --help").read()
        assert os.popen(f"python {rllib_dir}/evaluate.py --help").read()
        assert os.popen(f"python {rllib_dir}/scripts.py example --help").read()
        assert os.popen(f"python {rllib_dir}/scripts.py example list --help").read()
        assert os.popen(f"python {rllib_dir}/scripts.py example run --help").read()

    def test_example_commands(self):
        assert os.popen(f"python {rllib_dir}/scripts.py example list").read()
        assert os.popen(f"python {rllib_dir}/scripts.py example list -f=ppo").read()
        assert os.popen(f"python {rllib_dir}/scripts.py example get atari-a2c").read()
        assert os.popen(
            f"python {rllib_dir}/scripts.py example run cartpole-simpleq-test"
        ).read()

    def test_yaml_run(self):
        assert os.popen(
            f"python {rllib_dir}/scripts.py train file tuned_examples/simple_q/"
            f"cartpole-simpleq-test.yaml"
        ).read()

    def test_python_run(self):
        assert os.popen(
            f"python {rllib_dir}/scripts.py train file tuned_examples/simple_q/"
            f"cartpole_simpleq_test.py "
            f"--stop={'timesteps_total': 50000, 'episode_reward_mean': 200}"
        ).read()

    def test_all_example_files_exist(self):
        """The 'example' command now knows about example files,
        so we check that they exist."""
        from ray.rllib.common import EXAMPLES

        for val in EXAMPLES.values():
            file = val["file"]
            assert os.path.exists(os.path.join(rllib_dir, file))


if __name__ == "__main__":
    import pytest

    # One can specify the specific TestCase class to run.
    # None for all unittest.TestCase classes in this file.
    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(["-v", __file__ + ("" if class_ is None else "::" + class_)]))
