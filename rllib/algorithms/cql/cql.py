import logging
from typing import Optional, Type

from ray.rllib.algorithms.cql.cql_tf_policy import CQLTFPolicy
from ray.rllib.algorithms.cql.cql_torch_policy import CQLTorchPolicy
from ray.rllib.algorithms.sac.sac import (
    SAC,
    SACConfig,
)
from ray.rllib.execution.rollout_ops import (
    synchronous_parallel_sample,
)
from ray.rllib.execution.train_ops import (
    multi_gpu_train_one_step,
    train_one_step,
)
from ray.rllib.utils.replay_buffers.utils import sample_min_n_steps_from_buffer
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import (
    DEPRECATED_VALUE,
    deprecation_warning,
    Deprecated,
)
from ray.rllib.utils.framework import try_import_tf, try_import_tfp
from ray.rllib.utils.metrics import (
    LAST_TARGET_UPDATE_TS,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_TRAINED,
    NUM_TARGET_UPDATES,
    TARGET_NET_UPDATE_TIMER,
    SYNCH_WORKER_WEIGHTS_TIMER,
)
from ray.rllib.utils.replay_buffers.utils import update_priorities_in_replay_buffer
from ray.rllib.utils.typing import ResultDict, AlgorithmConfigDict

tf1, tf, tfv = try_import_tf()
tfp = try_import_tfp()
logger = logging.getLogger(__name__)


class CQLConfig(SACConfig):
    """Defines a configuration class from which a CQL Trainer can be built.

    Example:
        >>> config = CQLConfig().training(gamma=0.9, lr=0.01)\
        ...     .resources(num_gpus=0)\
        ...     .rollouts(num_rollout_workers=4)
        >>> print(config.to_dict())
        >>> # Build a Trainer object from the config and run 1 training iteration.
        >>> trainer = config.build(env="CartPole-v1")
        >>> trainer.train()
    """

    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or CQL)

        # fmt: off
        # __sphinx_doc_begin__
        # CQL-specific config settings:
        self.bc_iters = 20000
        self.temperature = 1.0
        self.num_actions = 10
        self.lagrangian = False
        self.lagrangian_thresh = 5.0
        self.min_q_weight = 5.0

        # Changes to Trainer's/SACConfig's default:
        # .offline_data()
        self.off_policy_estimation_methods = {}

        # .reporting()
        self.min_sample_timesteps_per_iteration = 0
        self.min_train_timesteps_per_iteration = 100
        # fmt: on
        # __sphinx_doc_end__

        self.timesteps_per_iteration = DEPRECATED_VALUE

    def training(
        self,
        *,
        bc_iters: Optional[int] = None,
        temperature: Optional[float] = None,
        num_actions: Optional[int] = None,
        lagrangian: Optional[bool] = None,
        lagrangian_thresh: Optional[float] = None,
        min_q_weight: Optional[float] = None,
        **kwargs,
    ) -> "CQLConfig":
        """Sets the training-related configuration.

        Args:
            bc_iters: Number of iterations with Behavior Cloning pretraining.
            temperature: CQL loss temperature.
            num_actions: Number of actions to sample for CQL loss
            lagrangian: Whether to use the Lagrangian for Alpha Prime (in CQL loss).
            lagrangian_thresh: Lagrangian threshold.
            min_q_weight: in Q weight multiplier.

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if bc_iters is not None:
            self.bc_iters = bc_iters
        if temperature is not None:
            self.temperature = temperature
        if num_actions is not None:
            self.num_actions = num_actions
        if lagrangian is not None:
            self.lagrangian = lagrangian
        if lagrangian_thresh is not None:
            self.lagrangian_thresh = lagrangian_thresh
        if min_q_weight is not None:
            self.min_q_weight = min_q_weight

        return self


class CQL(SAC):
    """CQL (derived from SAC)."""

    @classmethod
    @override(SAC)
    def get_default_config(cls) -> AlgorithmConfigDict:
        return CQLConfig().to_dict()

    @override(SAC)
    def validate_config(self, config: AlgorithmConfigDict) -> None:
        # First check, whether old `timesteps_per_iteration` is used. If so
        # convert right away as for CQL, we must measure in training timesteps,
        # never sampling timesteps (CQL does not sample).
        if config.get("timesteps_per_iteration", DEPRECATED_VALUE) != DEPRECATED_VALUE:
            deprecation_warning(
                old="timesteps_per_iteration",
                new="min_train_timesteps_per_iteration",
                error=False,
            )
            config["min_train_timesteps_per_iteration"] = config[
                "timesteps_per_iteration"
            ]
            config["timesteps_per_iteration"] = DEPRECATED_VALUE

        # Call super's validation method.
        super().validate_config(config)

        if config["num_gpus"] > 1:
            raise ValueError("`num_gpus` > 1 not yet supported for CQL!")

        # CQL-torch performs the optimizer steps inside the loss function.
        # Using the multi-GPU optimizer will therefore not work (see multi-GPU
        # check above) and we must use the simple optimizer for now.
        if config["simple_optimizer"] is not True and config["framework"] == "torch":
            config["simple_optimizer"] = True

        if config["framework"] in ["tf", "tf2", "tfe"] and tfp is None:
            logger.warning(
                "You need `tensorflow_probability` in order to run CQL! "
                "Install it via `pip install tensorflow_probability`. Your "
                f"tf.__version__={tf.__version__ if tf else None}."
                "Trying to import tfp results in the following error:"
            )
            try_import_tfp(error=True)

    @override(SAC)
    def get_default_policy_class(self, config: AlgorithmConfigDict) -> Type[Policy]:
        if config["framework"] == "torch":
            return CQLTorchPolicy
        else:
            return CQLTFPolicy

    @override(SAC)
    def training_step(self) -> ResultDict:
        # Collect SampleBatches from sample workers.
        batch = synchronous_parallel_sample(worker_set=self.workers)
        batch = batch.as_multi_agent()
        self._counters[NUM_AGENT_STEPS_SAMPLED] += batch.agent_steps()
        self._counters[NUM_ENV_STEPS_SAMPLED] += batch.env_steps()
        # Add batch to replay buffer.
        self.local_replay_buffer.add(batch)

        # Sample training batch from replay buffer.
        train_batch = sample_min_n_steps_from_buffer(
            self.local_replay_buffer,
            self.config["train_batch_size"],
            count_by_agent_steps=self._by_agent_steps,
        )

        # Old-style replay buffers return None if learning has not started
        if not train_batch:
            return {}

        # Postprocess batch before we learn on it.
        post_fn = self.config.get("before_learn_on_batch") or (lambda b, *a: b)
        train_batch = post_fn(train_batch, self.workers, self.config)

        # Learn on training batch.
        # Use simple optimizer (only for multi-agent or tf-eager; all other
        # cases should use the multi-GPU optimizer, even if only using 1 GPU)
        if self.config.get("simple_optimizer") is True:
            train_results = train_one_step(self, train_batch)
        else:
            train_results = multi_gpu_train_one_step(self, train_batch)

        # Update replay buffer priorities.
        update_priorities_in_replay_buffer(
            self.local_replay_buffer,
            self.config,
            train_batch,
            train_results,
        )

        # Update target network every `target_network_update_freq` training steps.
        cur_ts = self._counters[
            NUM_AGENT_STEPS_TRAINED if self._by_agent_steps else NUM_ENV_STEPS_TRAINED
        ]
        last_update = self._counters[LAST_TARGET_UPDATE_TS]
        if cur_ts - last_update >= self.config["target_network_update_freq"]:
            with self._timers[TARGET_NET_UPDATE_TIMER]:
                to_update = self.workers.local_worker().get_policies_to_train()
                self.workers.local_worker().foreach_policy_to_train(
                    lambda p, pid: pid in to_update and p.update_target()
                )
            self._counters[NUM_TARGET_UPDATES] += 1
            self._counters[LAST_TARGET_UPDATE_TS] = cur_ts

        # Update remote workers's weights after learning on local worker
        if self.workers.remote_workers():
            with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
                self.workers.sync_weights()

        # Return all collected metrics for the iteration.
        return train_results


class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(CQLConfig().to_dict())

    @Deprecated(
        old="ray.rllib.algorithms.cql.cql::DEFAULT_CONFIG",
        new="ray.rllib.algorithms.cql.cql::CQLConfig(...)",
        error=False,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
CQL_DEFAULT_CONFIG = DEFAULT_CONFIG
