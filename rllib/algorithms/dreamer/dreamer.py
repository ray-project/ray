import logging
import numpy as np
import random
from typing import Optional

from ray.rllib.agents.trainer_config import TrainerConfig
from ray.rllib.algorithms.dreamer.dreamer_torch_policy import DreamerTorchPolicy
from ray.rllib.agents.trainer import Trainer
from ray.rllib.execution.common import STEPS_SAMPLED_COUNTER, _get_shared_metrics
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.evaluation.metrics import collect_metrics
from ray.rllib.algorithms.dreamer.dreamer_model import DreamerModel
from ray.rllib.execution.rollout_ops import (
    ParallelRollouts,
    synchronous_parallel_sample,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.typing import (
    PartialTrainerConfigDict,
    SampleBatchType,
    TrainerConfigDict,
    ResultDict,
)

logger = logging.getLogger(__name__)


class DREAMERConfig(TrainerConfig):
    """Defines a PPOTrainer configuration class from which a PPOTrainer can be built.

    Example:
        >>> from ray.rllib.algorithms.dreamer import DREAMERConfig
        >>> config = DREAMERConfig().training(gamma=0.9, lr=0.01)\
        ...     .resources(num_gpus=0)\
        ...     .rollouts(num_rollout_workers=4)
        >>> print(config.to_dict())
        >>> # Build a Trainer object from the config and run 1 training iteration.
        >>> trainer = config.build(env="CartPole-v1")
        >>> trainer.train()

    Example:
        >>> from ray import tune
        >>> from ray.rllib.algorithms.dreamer import DREAMERConfig
        >>> config = DREAMERConfig()
        >>> # Print out some default values.
        >>> print(config.clip_param)
        >>> # Update the config object.
        >>> config.training(lr=tune.grid_search([0.001, 0.0001]), clip_param=0.2)
        >>> # Set the config object's env.
        >>> config.environment(env="CartPole-v1")
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.run(
        ...     "DREAMER",
        ...     stop={"episode_reward_mean": 200},
        ...     config=config.to_dict(),
        ... )
    """

    def __init__(self):
        """Initializes a PPOConfig instance."""
        super().__init__(trainer_class=DREAMERTrainer)

        # fmt: off
        # __sphinx_doc_begin__
        # Dreamer specific settings:
        self.td_model_lr = 6e-4
        self.actor_lr = 8e-5
        self.critic_lr = 8e-5
        self.grad_clip = 100.0
        self.lambda_ = 0.95
        self.dreamer_train_iters = 100
        self.batch_size = 50
        self.batch_length = 50
        self.imagine_horizon = 15
        self.free_nats = 3.0
        self.kl_coeff = 1.0
        self.prefill_timesteps = 5000
        self.explore_noise = 0.3
        self.dreamer_model = {
            "custom_model": DreamerModel,
            # RSSM/PlaNET parameters
            "deter_size": 200,
            "stoch_size": 30,
            # CNN Decoder Encoder
            "depth_size": 32,
            # General Network Parameters
            "hidden_size": 400,
            # Action STD
            "action_init_std": 5.0,
        }

        # Override some of TrainerConfig's default values with PPO-specific values.
        # .rollouts()
        self.num_workers = 0
        self.num_envs_per_worker = 1
        self.horizon = 1000
        self.batch_mode = "complete_episodes"
        self.clip_actions = False

        # .training()
        self.gamma = 0.99

        # .environment()
        self.env_config = {
            # Repeats action send by policy for frame_skip times in env
            "frame_skip": 2,
        }

        # __sphinx_doc_end__
        # fmt: on

    @override(TrainerConfig)
    def training(
        self,
        *,
        td_model_lr: Optional[float] = None,
        actor_lr: Optional[float] = None,
        critic_lr: Optional[float] = None,
        grad_clip: Optional[float] = None,
        lambda_: Optional[float] = None,
        dreamer_train_iters: Optional[int] = None,
        batch_size: Optional[int] = None,
        batch_length: Optional[int] = None,
        imagine_horizon: Optional[int] = None,
        free_nats: Optional[float] = None,
        kl_coeff: Optional[float] = None,
        prefill_timesteps: Optional[int] = None,
        explore_noise: Optional[float] = None,
        dreamer_model: Optional[dict] = None,
        **kwargs,
    ) -> "DREAMERConfig":
        """

        Args:
            td_model_lr: PlaNET (transition dynamics) model learning rate.
            actor_lr: Actor model learning rate.
            critic_lr: Critic model learning rate.
            grad_clip: If specified, clip the global norm of gradients by this amount.
            lambda_: The GAE (lambda) parameter.
            dreamer_train_iters: Training iterations per data collection from real env.
            batch_size: Number of episodes to sample for loss calculation.
            batch_length: Length of each episode to sample for loss calculation.
            imagine_horizon: Imagination horizon for training Actor and Critic.
            free_nats: Free nats.
            kl_coeff: KL coefficient for the model Loss.
            prefill_timesteps: Prefill timesteps.
            explore_noise: Exploration Gaussian noise.
            dreamer_model: Custom model config.

        Returns:

        """

        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if td_model_lr is not None:
            self.td_model_lr = td_model_lr
        if actor_lr is not None:
            self.actor_lr = actor_lr
        if critic_lr is not None:
            self.critic_lr = critic_lr
        if grad_clip is not None:
            self.grad_clip = grad_clip
        if lambda_ is not None:
            self.lambda_ = lambda_
        if dreamer_train_iters is not None:
            self.dreamer_train_iters = dreamer_train_iters
        if batch_size is not None:
            self.batch_size = batch_size
        if batch_length is not None:
            self.batch_length = batch_length
        if imagine_horizon is not None:
            self.imagine_horizon = imagine_horizon
        if free_nats is not None:
            self.free_nats = free_nats
        if kl_coeff is not None:
            self.kl_coeff = kl_coeff
        if prefill_timesteps is not None:
            self.prefill_timesteps = prefill_timesteps
        if explore_noise is not None:
            self.explore_noise = explore_noise
        if dreamer_model is not None:
            self.dreamer_model = dreamer_model

        return self


def _postprocess_gif(gif: np.ndarray):
    """Process provided gif to a format that can be logged to Tensorboard."""
    gif = np.clip(255 * gif, 0, 255).astype(np.uint8)
    B, T, C, H, W = gif.shape
    frames = gif.transpose((1, 2, 3, 0, 4)).reshape((1, T, C, H, B * W))
    return frames


class EpisodicBuffer(object):
    def __init__(self, max_length: int = 1000, length: int = 50):
        """Stores episodes and samples chunks of size ``length`` from episodes.

        Args:
            max_length: Maximum episodes it can store
            length: Episode chunking length in sample()
        """

        # Stores all episodes into a list: List[SampleBatchType]
        self.episodes = []
        self.max_length = max_length
        self.timesteps = 0
        self.length = length

    def add(self, batch: SampleBatchType):
        """Splits a SampleBatch into episodes and adds episodes to the episode buffer.

        Args:
            batch: SampleBatch to be added
        """

        self.timesteps += batch.count
        episodes = batch.split_by_episode()
        self.episodes.extend(episodes)

        if len(self.episodes) > self.max_length:
            delta = len(self.episodes) - self.max_length
            # Drop oldest episodes
            self.episodes = self.episodes[delta:]

    def sample(self, batch_size: int):
        """Samples [batch_size, length] from the list of episodes

        Args:
            batch_size: batch_size to be sampled
        """
        episodes_buffer = []
        while len(episodes_buffer) < batch_size:
            rand_index = random.randint(0, len(self.episodes) - 1)
            episode = self.episodes[rand_index]
            if episode.count < self.length:
                continue
            available = episode.count - self.length
            index = int(random.randint(0, available))
            episodes_buffer.append(episode[index : index + self.length])

        return SampleBatch.concat_samples(episodes_buffer)


def total_sampled_timesteps(worker):
    return worker.policy_map[DEFAULT_POLICY_ID].global_timestep


class DreamerIteration:
    def __init__(
        self, worker, episode_buffer, dreamer_train_iters, batch_size, act_repeat
    ):
        self.worker = worker
        self.episode_buffer = episode_buffer
        self.dreamer_train_iters = dreamer_train_iters
        self.repeat = act_repeat
        self.batch_size = batch_size

    def __call__(self, samples):
        # Dreamer training loop.
        for n in range(self.dreamer_train_iters):
            print(f"sub-iteration={n}/{self.dreamer_train_iters}")
            batch = self.episode_buffer.sample(self.batch_size)
            # if n == self.dreamer_train_iters - 1:
            #     batch["log_gif"] = True
            fetches = self.worker.learn_on_batch(batch)

        # Custom Logging
        policy_fetches = fetches[DEFAULT_POLICY_ID]["learner_stats"]
        if "log_gif" in policy_fetches:
            gif = policy_fetches["log_gif"]
            policy_fetches["log_gif"] = self.postprocess_gif(gif)

        # Metrics Calculation
        metrics = _get_shared_metrics()
        metrics.info[LEARNER_INFO] = fetches
        metrics.counters[STEPS_SAMPLED_COUNTER] = self.episode_buffer.timesteps
        metrics.counters[STEPS_SAMPLED_COUNTER] *= self.repeat
        res = collect_metrics(local_worker=self.worker)
        res["info"] = metrics.info
        res["info"].update(metrics.counters)
        res["timesteps_total"] = metrics.counters[STEPS_SAMPLED_COUNTER]

        self.episode_buffer.add(samples)
        return res

    def postprocess_gif(self, gif: np.ndarray):
        return _postprocess_gif(gif=gif)


class DREAMERTrainer(Trainer):
    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DREAMERConfig().to_dict()

    @override(Trainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        # Call super's validation method.
        super().validate_config(config)

        config["action_repeat"] = config["env_config"]["frame_skip"]
        if config["num_gpus"] > 1:
            raise ValueError("`num_gpus` > 1 not yet supported for Dreamer!")
        if config["framework"] != "torch":
            raise ValueError("Dreamer not supported in Tensorflow yet!")
        if config["batch_mode"] != "complete_episodes":
            raise ValueError("truncate_episodes not supported")
        if config["num_workers"] != 0:
            raise ValueError("Distributed Dreamer not supported yet!")
        if config["clip_actions"]:
            raise ValueError("Clipping is done inherently via policy tanh!")
        if config["dreamer_train_iters"] <= 0:
            raise ValueError(
                "`dreamer_train_iters` must be a positive integer. "
                f"Received {config['dreamer_train_iters']} instead."
            )
        if config["action_repeat"] > 1:
            config["horizon"] = config["horizon"] / config["action_repeat"]

    @override(Trainer)
    def get_default_policy_class(self, config: TrainerConfigDict):
        return DreamerTorchPolicy

    @override(Trainer)
    def setup(self, config: PartialTrainerConfigDict):
        super().setup(config)
        # `training_iteration` implementation: Setup buffer in `setup`, not
        # in `execution_plan` (deprecated).
        if self.config["_disable_execution_plan_api"] is True:
            self.local_replay_buffer = EpisodicBuffer(length=config["batch_length"])

            # Prefill episode buffer with initial exploration (uniform sampling)
            while (
                total_sampled_timesteps(self.workers.local_worker())
                < self.config["prefill_timesteps"]
            ):
                samples = self.workers.local_worker().sample()
                self.local_replay_buffer.add(samples)

    @staticmethod
    @override(Trainer)
    def execution_plan(workers, config, **kwargs):
        assert (
            len(kwargs) == 0
        ), "Dreamer execution_plan does NOT take any additional parameters"

        # Special replay buffer for Dreamer agent.
        episode_buffer = EpisodicBuffer(length=config["batch_length"])

        local_worker = workers.local_worker()

        # Prefill episode buffer with initial exploration (uniform sampling)
        while total_sampled_timesteps(local_worker) < config["prefill_timesteps"]:
            samples = local_worker.sample()
            episode_buffer.add(samples)

        batch_size = config["batch_size"]
        dreamer_train_iters = config["dreamer_train_iters"]
        act_repeat = config["action_repeat"]

        rollouts = ParallelRollouts(workers)
        rollouts = rollouts.for_each(
            DreamerIteration(
                local_worker,
                episode_buffer,
                dreamer_train_iters,
                batch_size,
                act_repeat,
            )
        )
        return rollouts

    @override(Trainer)
    def training_iteration(self) -> ResultDict:
        local_worker = self.workers.local_worker()

        # Number of sub-iterations for Dreamer
        dreamer_train_iters = self.config["dreamer_train_iters"]
        batch_size = self.config["batch_size"]
        action_repeat = self.config["action_repeat"]

        # Collect SampleBatches from rollout workers.
        batch = synchronous_parallel_sample(worker_set=self.workers)

        fetches = {}

        # Dreamer training loop.
        # Run multiple sub-iterations for each training iteration.
        for n in range(dreamer_train_iters):
            print(f"sub-iteration={n}/{dreamer_train_iters}")
            batch = self.local_replay_buffer.sample(batch_size)
            fetches = local_worker.learn_on_batch(batch)

        if fetches:
            # Custom Logging
            policy_fetches = fetches[DEFAULT_POLICY_ID]["learner_stats"]
            if "log_gif" in policy_fetches:
                gif = policy_fetches["log_gif"]
                policy_fetches["log_gif"] = self._postprocess_gif(gif)

        self._counters[STEPS_SAMPLED_COUNTER] = (
            self.local_replay_buffer.timesteps * action_repeat
        )

        self.local_replay_buffer.add(batch)

        return fetches

    def _compile_step_results(self, *args, **kwargs):
        results = super()._compile_step_results(*args, **kwargs)
        results["timesteps_total"] = self._counters[STEPS_SAMPLED_COUNTER]
        return results


# Deprecated: Use ray.rllib.algorithms.dreamer.DREAMERConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(DREAMERConfig().to_dict())

    @Deprecated(
        old="ray.rllib.algorithms.dreamer.dreamer.DEFAULT_CONFIG",
        new="ray.rllib.algorithms.dreamer.dreamer.DREAMERConfig(...)",
        error=False,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
