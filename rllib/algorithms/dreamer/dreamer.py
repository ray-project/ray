import logging
import numpy as np
import random
from typing import Optional, Type

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.dreamer.dreamer_torch_policy import DreamerTorchPolicy
from ray.rllib.execution.common import STEPS_SAMPLED_COUNTER, _get_shared_metrics
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import (
    DEFAULT_POLICY_ID,
    concat_samples,
    convert_ma_batch_to_sample_batch,
)
from ray.rllib.evaluation.metrics import collect_metrics
from ray.rllib.algorithms.dreamer.dreamer_model import DreamerModel
from ray.rllib.execution.rollout_ops import (
    synchronous_parallel_sample,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    SAMPLE_TIMER,
)
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.typing import (
    ResultDict,
)
from ray.rllib.utils.replay_buffers import ReplayBuffer, StorageUnit

logger = logging.getLogger(__name__)


class DreamerConfig(AlgorithmConfig):
    """Defines a configuration class from which a Dreamer Algorithm can be built.

    Example:
        >>> from ray.rllib.algorithms.dreamer import DreamerConfig
        >>> config = DreamerConfig().training(gamma=0.9, lr=0.01)  # doctest: +SKIP
        >>> config = config.resources(num_gpus=0)  # doctest: +SKIP
        >>> config = config.rollouts(num_rollout_workers=4)  # doctest: +SKIP
        >>> print(config.to_dict())  # doctest: +SKIP
        >>> # Build a Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build(env="CartPole-v1")  # doctest: +SKIP
        >>> algo.train()  # doctest: +SKIP

    Example:
        >>> from ray import air
        >>> from ray import tune
        >>> from ray.rllib.algorithms.dreamer import DreamerConfig
        >>> config = DreamerConfig()
        >>> # Print out some default values.
        >>> print(config.clip_param)  # doctest: +SKIP
        >>> # Update the config object.
        >>> config = config.training(  # doctest: +SKIP
        ...     lr=tune.grid_search([0.001, 0.0001]), clip_param=0.2)
        >>> # Set the config object's env.
        >>> config = config.environment(env="CartPole-v1")  # doctest: +SKIP
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.Tuner(  # doctest: +SKIP
        ...     "Dreamer",
        ...     run_config=air.RunConfig(stop={"episode_reward_mean": 200}),
        ...     param_space=config.to_dict(),
        ... ).fit()
    """

    def __init__(self):
        """Initializes a PPOConfig instance."""
        super().__init__(algo_class=Dreamer)

        # fmt: off
        # __sphinx_doc_begin__
        # Dreamer specific settings:
        self.td_model_lr = 6e-4
        self.actor_lr = 8e-5
        self.critic_lr = 8e-5

        self.grad_clip = 100.0
        # Note: Only when using _enable_learner_api=True can the clipping mode be
        # configured by the user. On the old API stack, RLlib will always clip by
        # global_norm, no matter the value of `grad_clip_by`.
        self.grad_clip_by = "global_norm"

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

        # Override some of AlgorithmConfig's default values with Dreamer-specific
        # values.
        # .rollouts()
        self.num_envs_per_worker = 1
        self.batch_mode = "complete_episodes"
        self.clip_actions = False

        # .training()
        self.gamma = 0.99
        # Number of timesteps to collect from rollout workers before we start
        # sampling from replay buffers for learning. Whether we count this in agent
        # steps  or environment steps depends on config.multi_agent(count_steps_by=..).
        self.num_steps_sampled_before_learning_starts = 0

        # .environment()
        self.env_config.update({
            # Repeats action send by policy for frame_skip times in env
            "frame_skip": 2,
        })

        # .exploration()
        # This dreamer implementation does not need an exploration config
        self.exploration_config = {}

        # __sphinx_doc_end__
        # fmt: on

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        td_model_lr: Optional[float] = NotProvided,
        actor_lr: Optional[float] = NotProvided,
        critic_lr: Optional[float] = NotProvided,
        grad_clip: Optional[float] = NotProvided,
        lambda_: Optional[float] = NotProvided,
        dreamer_train_iters: Optional[int] = NotProvided,
        batch_size: Optional[int] = NotProvided,
        batch_length: Optional[int] = NotProvided,
        imagine_horizon: Optional[int] = NotProvided,
        free_nats: Optional[float] = NotProvided,
        kl_coeff: Optional[float] = NotProvided,
        prefill_timesteps: Optional[int] = NotProvided,
        explore_noise: Optional[float] = NotProvided,
        dreamer_model: Optional[dict] = NotProvided,
        num_steps_sampled_before_learning_starts: Optional[int] = NotProvided,
        **kwargs,
    ) -> "DreamerConfig":
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
            num_steps_sampled_before_learning_starts: Number of timesteps to collect
                from rollout workers before we start sampling from replay buffers for
                learning. Whether we count this in agent steps  or environment steps
                depends on config.multi_agent(count_steps_by=..).

        Returns:

        """

        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if td_model_lr is not NotProvided:
            self.td_model_lr = td_model_lr
        if actor_lr is not NotProvided:
            self.actor_lr = actor_lr
        if critic_lr is not NotProvided:
            self.critic_lr = critic_lr
        if grad_clip is not NotProvided:
            self.grad_clip = grad_clip
        if lambda_ is not NotProvided:
            self.lambda_ = lambda_
        if dreamer_train_iters is not NotProvided:
            self.dreamer_train_iters = dreamer_train_iters
        if batch_size is not NotProvided:
            self.batch_size = batch_size
        if batch_length is not NotProvided:
            self.batch_length = batch_length
        if imagine_horizon is not NotProvided:
            self.imagine_horizon = imagine_horizon
        if free_nats is not NotProvided:
            self.free_nats = free_nats
        if kl_coeff is not NotProvided:
            self.kl_coeff = kl_coeff
        if prefill_timesteps is not NotProvided:
            self.prefill_timesteps = prefill_timesteps
        if explore_noise is not NotProvided:
            self.explore_noise = explore_noise
        if dreamer_model is not NotProvided:
            self.dreamer_model = dreamer_model
        if num_steps_sampled_before_learning_starts is not NotProvided:
            self.num_steps_sampled_before_learning_starts = (
                num_steps_sampled_before_learning_starts
            )

        return self

    @override(AlgorithmConfig)
    def validate(self) -> None:
        # Call super's validation method.
        super().validate()

        if self.num_gpus > 1:
            raise ValueError("`num_gpus` > 1 not yet supported for Dreamer!")
        if self.framework_str != "torch":
            raise ValueError("Dreamer not supported in Tensorflow yet!")
        if self.batch_mode != "complete_episodes":
            raise ValueError("truncate_episodes not supported")
        if self.num_rollout_workers != 0:
            raise ValueError("Distributed Dreamer not supported yet!")
        if self.clip_actions:
            raise ValueError("Clipping is done inherently via policy tanh!")
        if self.dreamer_train_iters <= 0:
            raise ValueError(
                "`dreamer_train_iters` must be a positive integer. "
                f"Received {self.dreamer_train_iters} instead."
            )
        if self.env_config.get("frame_skip", 0) > 1:
            self.imagine_horizon //= self.env_config["frame_skip"]


def _postprocess_gif(gif: np.ndarray):
    """Process provided gif to a format that can be logged to Tensorboard."""
    gif = np.clip(255 * gif, 0, 255).astype(np.uint8)
    B, T, C, H, W = gif.shape
    frames = gif.transpose((1, 2, 3, 0, 4)).reshape((1, T, C, H, B * W))
    return frames


class EpisodeSequenceBuffer(ReplayBuffer):
    def __init__(self, capacity: int = 1000, replay_sequence_length: int = 50):
        """Stores episodes and samples sequences of size `replay_sequence_length`.

        Args:
            capacity: Maximum number of episodes this buffer can store
            replay_sequence_length: Episode chunking length in sample()
        """
        super().__init__(capacity=capacity, storage_unit=StorageUnit.EPISODES)
        self.replay_sequence_length = replay_sequence_length

    def sample(self, num_items: int):
        """Samples [batch_size, length] from the list of episodes

        Args:
            num_items: batch_size to be sampled
        """
        episodes_buffer = []
        while len(episodes_buffer) < num_items:
            episode = super().sample(1)
            if episode.count < self.replay_sequence_length:
                continue
            available = episode.count - self.replay_sequence_length
            index = int(random.randint(0, available))
            episodes_buffer.append(episode[index : index + self.replay_sequence_length])

        return concat_samples(episodes_buffer)


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

        # Update target network every `target_network_update_freq` sample steps.
        cur_ts = self._counters[
            NUM_AGENT_STEPS_SAMPLED
            if self.config.count_steps_by == "agent_steps"
            else NUM_ENV_STEPS_SAMPLED
        ]

        if cur_ts > self.config.num_steps_sampled_before_learning_starts:
            # Dreamer training loop.
            for n in range(self.dreamer_train_iters):
                print(f"sub-iteration={n}/{self.dreamer_train_iters}")
                batch = self.episode_buffer.sample(self.batch_size)
                fetches = self.worker.learn_on_batch(batch)
        else:
            fetches = {}

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


@Deprecated(
    old="rllib/algorithms/dreamer/",
    new="rllib_contrib/dreamer/",
    help=ALGO_DEPRECATION_WARNING,
    error=False,
)
class Dreamer(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return DreamerConfig()

    @classmethod
    @override(Algorithm)
    def get_default_policy_class(
        cls, config: AlgorithmConfig
    ) -> Optional[Type[Policy]]:
        return DreamerTorchPolicy

    @override(Algorithm)
    def setup(self, config: AlgorithmConfig):
        super().setup(config)

        # Setup buffer.
        self.local_replay_buffer = EpisodeSequenceBuffer(
            replay_sequence_length=config["batch_length"]
        )

        # Prefill episode buffer with initial exploration (uniform sampling)
        while (
            total_sampled_timesteps(self.workers.local_worker())
            < self.config.prefill_timesteps
        ):
            samples = self.workers.local_worker().sample()
            # Dreamer only ever has one policy and we receive MA batches when
            # connectors are on
            samples = convert_ma_batch_to_sample_batch(samples)
            self.local_replay_buffer.add(samples)

    @override(Algorithm)
    def training_step(self) -> ResultDict:
        local_worker = self.workers.local_worker()

        # Number of sub-iterations for Dreamer
        dreamer_train_iters = self.config.dreamer_train_iters
        batch_size = self.config.batch_size

        # Collect SampleBatches from rollout workers.
        with self._timers[SAMPLE_TIMER]:
            batch = synchronous_parallel_sample(worker_set=self.workers)
        self._counters[NUM_AGENT_STEPS_SAMPLED] += batch.agent_steps()
        self._counters[NUM_ENV_STEPS_SAMPLED] += batch.env_steps()

        fetches = {}

        # Update target network every `target_network_update_freq` sample steps.
        cur_ts = self._counters[
            NUM_AGENT_STEPS_SAMPLED
            if self.config.count_steps_by == "agent_steps"
            else NUM_ENV_STEPS_SAMPLED
        ]

        if cur_ts > self.config.num_steps_sampled_before_learning_starts:
            # Dreamer training loop.
            # Run multiple sub-iterations for each training iteration.
            for n in range(dreamer_train_iters):
                print(f"sub-iteration={n}/{dreamer_train_iters}")
                batch = self.local_replay_buffer.sample(batch_size)
                fetches = local_worker.learn_on_batch(batch)

            if fetches:
                # Custom logging.
                policy_fetches = fetches[DEFAULT_POLICY_ID]["learner_stats"]
                if "log_gif" in policy_fetches:
                    gif = policy_fetches["log_gif"]
                    policy_fetches["log_gif"] = self._postprocess_gif(gif)

        self.local_replay_buffer.add(batch)

        return fetches
