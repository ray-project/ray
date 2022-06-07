import unittest
import ray
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.offline.estimators import (
    ImportanceSampling,
    WeightedImportanceSampling,
    DirectMethod,
    DoublyRobust,
)
from ray.rllib.offline.json_reader import JsonReader
from pathlib import Path
import os
import numpy as np
import gym
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.typing import SampleBatchType
from typing import Generator, Tuple, Union


def train_test_split(
    batch: SampleBatchType,
    k: Union[float, int] = 0.0,
) -> Generator[Tuple[SampleBatch], None, None]:
    """Utility function that returns either a train/test split or
    a k-fold cross validation generator over episodes from the given batch.
    By default, `k` is set to 0.0, which sets eval_batch = batch
    and train_batch to an empty SampleBatch.

    Args:
        batch: A SampleBatch of episodes to split
        k: train/test split parameter; if k < 1, split the batch into
        `(1 - k) * num_episodes` eval batches and `k * num_episodes` train batches;
        if k > 1 split the batch into `k` folds fro cross-validation

    Returns:
        A tuple with two SampleBatches (eval_batch, train_batch)
    """
    episodes = batch.split_by_episode()
    n_episodes = len(episodes)
    assert (
        isinstance(k, float) and k >= 0 and k < 1 or isinstance(k, int)
    ), f" k: {k} must be either a float with 0.0 <= k < 1.0 or an int"
    if k < 1:
        train_episodes = episodes[: int(n_episodes * k)]
        eval_episodes = episodes[int(n_episodes * k) :]
        yield SampleBatch.concat_samples(eval_episodes), SampleBatch.concat_samples(
            train_episodes
        )
        return
    n_fold = n_episodes // k
    for i in range(k):
        train_episodes = episodes[: i * n_fold] + episodes[(i + 1) * n_fold :]
        if i != k - 1:
            eval_episodes = episodes[i * n_fold : (i + 1) * n_fold]
        else:
            # Append remaining episodes onto the last eval_episodes
            eval_episodes = episodes[i * n_fold :]
        yield SampleBatch.concat_samples(eval_episodes), SampleBatch.concat_samples(
            train_episodes
        )
    return


class TestOPE(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=4)
        rllib_dir = Path(__file__).parent.parent.parent.parent
        print("rllib dir={}".format(rllib_dir))
        data_file = os.path.join(rllib_dir, "tests/data/cartpole/large.json")
        print("data_file={} exists={}".format(data_file, os.path.isfile(data_file)))

        env_name = "CartPole-v0"
        cls.gamma = 0.99
        train_steps = 200000
        n_batches = 20  # Approx. equal to n_episodes
        n_eval_episodes = 100

        config = (
            DQNConfig()
            .rollouts(num_rollout_workers=3, batch_mode="complete_episodes")
            .environment(env=env_name)
            .training(gamma=cls.gamma)
            .exploration(
                explore=True,
                exploration_config={
                    "type": "SoftQ",
                    "temperature": 1.0,
                },
            )
            .framework("torch")
        )

        cls.trainer = config.build()
        ts = 0
        while ts < train_steps:
            results = cls.trainer.train()
            ts = results["timesteps_total"]
        print("total_time_s", results["total_time_s"])
        print("episode_reward_mean", results["episode_reward_mean"])

        # Read n_batches of data
        reader = JsonReader(data_file)
        cls.batch = reader.next()
        for _ in range(n_batches - 1):
            cls.batch = cls.batch.concat(reader.next())
        cls.n_episodes = len(cls.batch.split_by_episode())
        print("Episodes:", cls.n_episodes, "Steps:", cls.batch.count)

        cls.mean_ret = {}
        cls.std_ret = {}

        # Simulate Monte-Carlo rollouts
        mc_ret = []
        env = gym.make(env_name)
        for _ in range(n_eval_episodes):
            obs = env.reset()
            done = False
            rewards = []
            while not done:
                act = cls.trainer.compute_single_action(obs)
                obs, reward, done, _ = env.step(act)
                rewards.append(reward)
            ret = 0
            for r in reversed(rewards):
                ret = r + cls.gamma * ret
            mc_ret.append(ret)

        cls.mean_ret["simulation"] = np.mean(mc_ret)
        cls.std_ret["simulation"] = np.std(mc_ret)

        # Optional configs for the model-based estimators
        cls.k = 5
        cls.model_config = {"n_iters": 10}

    @classmethod
    def tearDownClass(cls):
        print("Mean:", cls.mean_ret)
        print("Stddev:", cls.std_ret)
        ray.shutdown()

    def test_is(self):
        name = "is"
        estimator = ImportanceSampling(
            name=name,
            policy=self.trainer.get_policy(),
            gamma=self.gamma,
        )
        estimator.process(self.batch)
        estimates = estimator.get_metrics()
        assert len(estimates) == self.n_episodes
        self.mean_ret[name] = np.mean([e.metrics["v_new"] for e in estimates])
        self.std_ret[name] = np.std([e.metrics["v_new"] for e in estimates])

    def test_wis(self):
        name = "wis"
        estimator = WeightedImportanceSampling(
            name=name,
            policy=self.trainer.get_policy(),
            gamma=self.gamma,
        )
        estimator.process(self.batch)
        estimates = estimator.get_metrics()
        assert len(estimates) == self.n_episodes
        self.mean_ret[name] = np.mean([e.metrics["v_new"] for e in estimates])
        self.std_ret[name] = np.std([e.metrics["v_new"] for e in estimates])

    def test_dm_qreg(self):
        name = "dm_qreg"
        estimator = DirectMethod(
            name=name,
            policy=self.trainer.get_policy(),
            gamma=self.gamma,
            q_model_type="qreg",
            **self.model_config,
        )
        for eval_batch, train_batch in train_test_split(self.batch, self.k):
            estimator.process(eval_batch, train_batch)
        estimates = estimator.get_metrics()
        assert len(estimates) == self.n_episodes
        self.mean_ret[name] = np.mean([e.metrics["v_new"] for e in estimates])
        self.std_ret[name] = np.std([e.metrics["v_new"] for e in estimates])

    def test_dm_fqe(self):
        name = "dm_fqe"
        estimator = DirectMethod(
            name=name,
            policy=self.trainer.get_policy(),
            gamma=self.gamma,
            q_model_type="fqe",
            **self.model_config,
        )
        for eval_batch, train_batch in train_test_split(self.batch, self.k):
            estimator.process(eval_batch, train_batch)
        estimates = estimator.get_metrics()
        assert len(estimates) == self.n_episodes
        self.mean_ret[name] = np.mean([e.metrics["v_new"] for e in estimates])
        self.std_ret[name] = np.std([e.metrics["v_new"] for e in estimates])

    def test_dr_qreg(self):
        name = "dr_qreg"
        estimator = DoublyRobust(
            name=name,
            policy=self.trainer.get_policy(),
            gamma=self.gamma,
            q_model_type="qreg",
            **self.model_config,
        )
        for eval_batch, train_batch in train_test_split(self.batch, self.k):
            estimator.process(eval_batch, train_batch)
        estimates = estimator.get_metrics()
        assert len(estimates) == self.n_episodes
        self.mean_ret[name] = np.mean([e.metrics["v_new"] for e in estimates])
        self.std_ret[name] = np.std([e.metrics["v_new"] for e in estimates])

    def test_dr_fqe(self):
        name = "dr_fqe"
        estimator = DoublyRobust(
            name=name,
            policy=self.trainer.get_policy(),
            gamma=self.gamma,
            q_model_type="fqe",
            **self.model_config,
        )
        for eval_batch, train_batch in train_test_split(self.batch, self.k):
            estimator.process(eval_batch, train_batch)
        estimates = estimator.get_metrics()
        assert len(estimates) == self.n_episodes
        self.mean_ret[name] = np.mean([e.metrics["v_new"] for e in estimates])
        self.std_ret[name] = np.std([e.metrics["v_new"] for e in estimates])

    def test_offline_evaluation_input(self):
        # Test that we can use input_=some_dataset and OPE in evaluation_config
        rllib_dir = Path(__file__).parent.parent.parent.parent
        print("rllib dir={}".format(rllib_dir))
        data_file = os.path.join(rllib_dir, "tests/data/cartpole/large.json")
        print("data_file={} exists={}".format(data_file, os.path.isfile(data_file)))

        env_name = "CartPole-v0"
        gamma = 0.99
        train_steps = 200000

        config = (
            DQNConfig()
            .rollouts(num_rollout_workers=2)
            .training(gamma=gamma)
            .environment(env=env_name)
            .offline_data(
                input_={
                    data_file: 0.5,
                    "sampler": 0.5,
                }
            )
            .exploration(
                explore=True,
                exploration_config={
                    "type": "SoftQ",
                    "temperature": 1.0,
                },
            )
            .evaluation(
                evaluation_interval=1,
                evaluation_num_workers=0,
                evaluation_config={
                    "input": os.path.join(rllib_dir, "tests/data/cartpole/small.json"),
                },
                off_policy_estimation_methods={
                    "is": {"type": ImportanceSampling},
                    "wis": {"type": WeightedImportanceSampling},
                },
            )
            .framework("torch")
            .rollouts(batch_mode="complete_episodes")
        )

        trainer = config.build()

        ts = 0
        while ts < train_steps:
            results = trainer.train()
            ts = results["timesteps_total"]
        print("total_time_s", results["total_time_s"])
        print("episode_reward_mean", results["episode_reward_mean"])
        print("Training", results["off_policy_estimator"])
        print("Evaluation", results["evaluation"]["off_policy_estimator"])
        assert not results["off_policy_estimator"]  # Should be None or {}

    def test_5_fold_cv_local_eval_worker(self):
        # 5-fold cv, local eval worker
        pass

    def test_5_fold_cv_eval_worker_0(self):
        # 5-fold cv, remote eval worker 0
        pass

    def test_5_fold_cv_eval_worker_3(self):
        # 5-fold cv, 3 eval workers
        pass

    def test_5_fold_cv_eval_worker_10(self):
        # 5-fold cv, 10 eval workers; can we use the extra 5 workers for something else?
        pass

    def test_ope_weird_replaybuffer(self):
        # Get OPE to work with distributed or other weird replay buffers
        pass

    def test_d3rply_cartpole_random(self):
        # Test OPE methods on d3rlpy cartpole-random
        pass

    def test_d3rlpy_cartpole_replay(self):
        # Test OPE methods on d3rlpy cartpole-random
        pass

    def test_cobs_mountaincar(self):
        # Test OPE methods on COBS MountainCar
        pass


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
