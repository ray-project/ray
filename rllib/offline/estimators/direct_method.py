import logging
from typing import Tuple, List, Dict, Union
from ray.rllib.offline.estimators.off_policy_estimator import OffPolicyEstimator
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI, override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import SampleBatchType
from ray.rllib.offline.estimators.fqe_torch_model import FQETorchModel
from ray.rllib.offline.estimators.qreg_torch_model import QRegTorchModel
from gym.spaces import Discrete
import numpy as np
from datetime import datetime
import os
import ray
from ray.tune.result import DEFAULT_RESULTS_DIR
import tempfile

from ray.tune import Trainable
from ray.tune.logger import UnifiedLogger

torch, nn = try_import_torch()

logger = logging.getLogger()


@DeveloperAPI
def train_test_split(
    batch: SampleBatchType,
    train_test_split_val: float = 0.0,
    k: int = 0,
) -> Tuple[List[List[SampleBatch]], List[List[SampleBatch]]]:
    """Utility function to split a batch into training and evaluation episodes.

    This function returns two lists with either a train/test split or
    a k-fold cross validation split over episodes from the given batch.
    By default, `k` is set to 0.0, which sets eval_batch = batch
    and train_batch to an empty SampleBatch.

    Args:
        batch: A SampleBatch of episodes to split
        train_test_split_val: Split the batch into a training batch with
        `train_test_split_val * n_episodes` episodes and an evaluation batch
        with `(1 - train_test_split_val) * n_episodes` episodes. If not
        specified, use `k` for k-fold cross validation instead.
        k: k-fold cross validation for training model and evaluating OPE.

    Returns:
        A tuple with two SampleBatches (eval_batch, train_batch)
    """
    episodes = batch.split_by_episode()
    n_episodes = len(episodes)
    # Train-test split
    if train_test_split_val:
        train_episodes = episodes[: int(n_episodes * train_test_split_val)]
        eval_episodes = episodes[int(n_episodes * train_test_split_val) :]
        return [eval_episodes], [train_episodes]
    # k-fold cv
    eval_batches = []
    train_batches = []
    assert n_episodes >= k, f"Not enough eval episodes in batch for {k}-fold cv!"
    n_fold = n_episodes // k
    for i in range(k):
        train_episodes = episodes[: i * n_fold] + episodes[(i + 1) * n_fold :]
        if i != k - 1:
            eval_episodes = episodes[i * n_fold : (i + 1) * n_fold]
        else:
            # Append remaining episodes onto the last eval_episodes
            eval_episodes = episodes[i * n_fold :]
        eval_batches.append(eval_episodes)
        train_batches.append(train_episodes)
    return eval_batches, train_batches


@DeveloperAPI
class DirectMethod(Trainable, OffPolicyEstimator):
    """The Direct Method estimator.

    DM estimator described in https://arxiv.org/pdf/1511.03722.pdf"""

    @DeveloperAPI
    def __init__(self, config):
        OffPolicyEstimator.__init__(self, config)
        # Setup logger
        timestr = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
        logdir_prefix = "{}_{}".format(self.name, timestr)
        if not os.path.exists(DEFAULT_RESULTS_DIR):
            os.makedirs(DEFAULT_RESULTS_DIR)
        logdir = tempfile.mkdtemp(prefix=logdir_prefix, dir=DEFAULT_RESULTS_DIR)

        def logger_creator(config):
            """Creates a Unified logger with the default prefix."""
            return UnifiedLogger(config, logdir, loggers=None)

        config.pop("policy")  # Neccessary for deepcopy within Trainable
        Trainable.__init__(self, config=config, logger_creator=logger_creator)

    @override(Trainable)
    def setup(self, config: Dict):
        # TODO (Rohan138): Add support for continuous action spaces
        assert isinstance(
            self.policy.action_space, Discrete
        ), "DM Estimator only supports discrete action spaces!"
        assert (
            self.policy.config["batch_mode"] == "complete_episodes"
        ), "DM Estimator only supports `batch_mode`=`complete_episodes`"

        # TODO (Rohan138): Add support for TF
        # TODO (Rohan138): Create config objects for OPE
        q_model_config = config.get("q_model", {"type": "fqe"})
        q_model_type = q_model_config.pop("type")
        if self.policy.framework == "torch":
            if q_model_type == "qreg":
                model_cls = QRegTorchModel
            elif q_model_type == "fqe":
                model_cls = FQETorchModel
            else:
                assert hasattr(
                    q_model_type, "estimate_q"
                ), "q_model_type must implement `estimate_q`!"
                assert hasattr(
                    q_model_type, "estimate_v"
                ), "q_model_type must implement `estimate_v`!"
        else:
            raise ValueError(
                f"{self.__class__.__name__}"
                "estimator only supports `policy.framework`=`torch`"
            )

        # Initialize training config
        self.n_iters = config.get("n_iters", 160)
        self.delta = config.get("delta", 1e-4)
        self.train_test_split_val = config.get("train_test_split_val", 0.0)
        assert (
            isinstance(self.train_test_split_val, float)
            and 0.0 <= self.train_test_split_val < 1.0
        )
        self.k = config.get("k", 1)
        assert isinstance(self.k, int) and self.k >= 1
        assert self.train_test_split_val != 0.0 or self.k != 1, (
            f"Either train_test_split_val: {self.train_test_split_val} != 0"
            f" or k: {self.k} != 1 for the {self.__class__.__name__} estimator!"
        )

        # TODO (Rohan138): Change {"type": ..., **config} design across RLlib
        # to {"type": ..., "config": {...}}
        # Create models
        self.model = model_cls(
            policy=self.policy,
            gamma=self.gamma,
            **q_model_config,
        )
        self.remote_models = []
        remote_model_cls = ray.remote(model_cls)
        self.remote_models = [
            remote_model_cls.remote(
                policy=self.policy, gamma=self.gamma, **q_model_config
            )
            for _ in range(self.k - 1)
        ]

        # If running with tune.run(), batch will be passed in to init config
        if config["batch"]:
            self.reset_config(config)

    @override(Trainable)
    def step(self):
        losses = [self.model.step()]
        losses.extend(ray.get([m.step.remote() for m in self.remote_models]))
        loss = np.mean(losses)
        done = self.iteration == self.n_iters - 1 or loss < self.delta
        return {"loss": loss, "done": done}

    @override(Trainable)
    def reset_config(self, new_config: Dict) -> bool:
        # Allows reusing remote models as ray actors
        self.eval_batches, train_batches = train_test_split(
            new_config["batch"],
            self.train_test_split_val,
            self.k,
        )
        self.model.set_batch(train_batches[0])
        for i, m in self.remote_models:
            m.set_batch.remote(train_batches[i + 1])
        return True

    @override(Trainable)
    def cleanup(self):
        for model in self.remote_models:
            model.__ray_terminate__.remote()

    @override(Trainable)
    def save_checkpoint(self, checkpoint_dir: str):
        checkpoint_path = os.path.join(
            checkpoint_dir, "checkpoint-{}".format(self.iteration)
        )
        model_state_dicts = [self.model.get_state_dict()]
        model_state_dicts.extend(
            ray.get([m.get_state_dict.remote() for m in self.remote_models])
        )
        torch.save(model_state_dicts, checkpoint_path)
        return checkpoint_path

    @override(Trainable)
    def load_checkpoint(self, checkpoint: Union[Dict, str]):
        model_state_dicts = torch.load(checkpoint)
        self.model.set_state_dict(model_state_dicts[0])
        for idx, m in enumerate(self.remote_models):
            m.set_state_dict(model_state_dicts[idx + 1])

    @override(OffPolicyEstimator)
    def estimate(self, batch: SampleBatchType) -> Dict[str, List]:
        self.check_can_estimate_for(batch)

        # Split data into train and eval batches and train model(s)
        self.reset_config(batch)
        for _ in range(self.n_iters):
            results = self.train()
            if results["done"]:
                break
        return self.evaluate()

    def evaluate(self) -> Dict[str, List]:
        # Calculate direct method OPE estimates
        estimates = {"v_old": [], "v_new": [], "v_gain": []}
        for idx, eval_episodes in enumerate(self.eval_batches):
            if idx != 0:
                self.model.set_state_dict(
                    self.remote_models[idx - 1].get_state_dict.remote()
                )
            for episode in eval_episodes:
                rewards = episode["rewards"]
                v_old = 0.0
                v_new = 0.0
                for t in range(episode.count):
                    v_old += rewards[t] * self.gamma ** t

                init_step = episode[0:1]
                init_obs = np.array([init_step[SampleBatch.OBS]])
                all_actions = np.arange(self.policy.action_space.n, dtype=float)
                init_step[SampleBatch.ACTIONS] = all_actions
                action_probs = np.exp(self.action_log_likelihood(init_step))
                v_value = self.model.estimate_v(init_obs, action_probs)
                v_new = convert_to_numpy(v_value).item()

                estimates["v_old"].append(v_old)
                estimates["v_new"].append(v_new)
                estimates["v_gain"].append(v_new / max(v_old, 1e-8))
        return estimates
