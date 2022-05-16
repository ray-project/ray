import logging
import numpy as np
from typing import Type

from ray.rllib.algorithms.cql.cql_tf_policy import CQLTFPolicy
from ray.rllib.algorithms.cql.cql_torch_policy import CQLTorchPolicy
from ray.rllib.agents.sac.sac import SACTrainer, DEFAULT_CONFIG as SAC_CONFIG
from ray.rllib.execution.train_ops import (
    multi_gpu_train_one_step,
    train_one_step,
)
from ray.rllib.offline.shuffled_input import ShuffledInput
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import merge_dicts
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import DEPRECATED_VALUE, deprecation_warning
from ray.rllib.utils.framework import try_import_tf, try_import_tfp
from ray.rllib.utils.metrics import (
    LAST_TARGET_UPDATE_TS,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED,
    NUM_TARGET_UPDATES,
    TARGET_NET_UPDATE_TIMER,
    SYNCH_WORKER_WEIGHTS_TIMER,
)
from ray.rllib.utils.replay_buffers.utils import update_priorities_in_replay_buffer
from ray.rllib.utils.typing import ResultDict, TrainerConfigDict

tf1, tf, tfv = try_import_tf()
tfp = try_import_tfp()
logger = logging.getLogger(__name__)

# fmt: off
# __sphinx_doc_begin__
CQL_DEFAULT_CONFIG = merge_dicts(
    SAC_CONFIG, {
        # You should override this to point to an offline dataset.
        "input": "sampler",
        # Switch off off-policy evaluation.
        "input_evaluation": [],
        # Number of iterations with Behavior Cloning Pretraining.
        "bc_iters": 20000,
        # CQL loss temperature.
        "temperature": 1.0,
        # Number of actions to sample for CQL loss.
        "num_actions": 10,
        # Whether to use the Lagrangian for Alpha Prime (in CQL loss).
        "lagrangian": False,
        # Lagrangian threshold.
        "lagrangian_thresh": 5.0,
        # Min Q weight multiplier.
        "min_q_weight": 5.0,
        "replay_buffer_config": {
            "_enable_replay_buffer_api": True,
            "type": "MultiAgentPrioritizedReplayBuffer",
            # Replay buffer should be larger or equal the size of the offline
            # dataset.
            "capacity": int(1e6),
        },
        # Reporting: As CQL is offline (no sampling steps), we need to limit
        # `self.train()` reporting by the number of steps trained (not sampled).
        "min_sample_timesteps_per_reporting": 0,
        "min_train_timesteps_per_reporting": 100,

        # Deprecated keys.
        # Use `replay_buffer_config.capacity` instead.
        "buffer_size": DEPRECATED_VALUE,
        # Use `min_sample_timesteps_per_reporting` and
        # `min_train_timesteps_per_reporting` instead.
        "timesteps_per_iteration": DEPRECATED_VALUE,
    })
# __sphinx_doc_end__
# fmt: on


class CQLTrainer(SACTrainer):
    """CQL (derived from SAC)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Add the entire dataset to Replay Buffer (global variable)
        reader = self.workers.local_worker().input_reader

        # For d4rl, add the D4RLReaders' dataset to the buffer.
        if isinstance(self.config["input"], str) and "d4rl" in self.config["input"]:
            dataset = reader.dataset
            self.local_replay_buffer.add(dataset)
        # For a list of files, add each file's entire content to the buffer.
        elif isinstance(reader, ShuffledInput):
            num_batches = 0
            total_timesteps = 0
            for batch in reader.child.read_all_files():
                num_batches += 1
                total_timesteps += len(batch)
                # Add NEXT_OBS if not available. This is slightly hacked
                # as for the very last time step, we will use next-obs=zeros
                # and therefore force-set DONE=True to avoid this missing
                # next-obs to cause learning problems.
                if SampleBatch.NEXT_OBS not in batch:
                    obs = batch[SampleBatch.OBS]
                    batch[SampleBatch.NEXT_OBS] = np.concatenate(
                        [obs[1:], np.zeros_like(obs[0:1])]
                    )
                    batch[SampleBatch.DONES][-1] = True
                self.local_replay_buffer.add_batch(batch)
            print(
                f"Loaded {num_batches} batches ({total_timesteps} ts) into the"
                " replay buffer, which has capacity "
                f"{self.local_replay_buffer.capacity}."
            )
        else:
            raise ValueError(
                "Unknown offline input! config['input'] must either be list of"
                " offline files (json) or a D4RL-specific InputReader "
                "specifier (e.g. 'd4rl.hopper-medium-v0')."
            )

    @classmethod
    @override(SACTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return CQL_DEFAULT_CONFIG

    @override(SACTrainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        # First check, whether old `timesteps_per_iteration` is used. If so
        # convert right away as for CQL, we must measure in training timesteps,
        # never sampling timesteps (CQL does not sample).
        if config.get("timesteps_per_iteration", DEPRECATED_VALUE) != DEPRECATED_VALUE:
            deprecation_warning(
                old="timesteps_per_iteration",
                new="min_train_timesteps_per_reporting",
                error=False,
            )
            config["min_train_timesteps_per_reporting"] = config[
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

    @override(SACTrainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
        if config["framework"] == "torch":
            return CQLTorchPolicy
        else:
            return CQLTFPolicy

    @override(SACTrainer)
    def training_iteration(self) -> ResultDict:

        # Sample training batch from replay buffer.
        train_batch = self.local_replay_buffer.sample(self.config["train_batch_size"])

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

        # Update target network every target_network_update_freq steps
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
