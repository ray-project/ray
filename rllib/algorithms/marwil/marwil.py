from typing import Type

from ray.rllib.agents.trainer import Trainer, with_common_config
from ray.rllib.algorithms.marwil.marwil_tf_policy import MARWILTFPolicy
from ray.rllib.execution.buffers.multi_agent_replay_buffer import MultiAgentReplayBuffer
from ray.rllib.execution.rollout_ops import (
    synchronous_parallel_sample,
)
from ray.rllib.execution.train_ops import (
    multi_gpu_train_one_step,
    train_one_step,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    WORKER_UPDATE_TIMER,
)
from ray.rllib.utils.typing import (
    PartialTrainerConfigDict,
    ResultDict,
    TrainerConfigDict,
)

# fmt: off
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # === Input settings ===
    # You should override this to point to an offline dataset
    # (see trainer.py).
    # The dataset may have an arbitrary number of timesteps
    # (and even episodes) per line.
    # However, each line must only contain consecutive timesteps in
    # order for MARWIL to be able to calculate accumulated
    # discounted returns. It is ok, though, to have multiple episodes in
    # the same line.
    "input": "sampler",
    # Use importance sampling estimators for reward.
    "input_evaluation": ["is", "wis"],

    # === Postprocessing/accum., discounted return calculation ===
    # If true, use the Generalized Advantage Estimator (GAE)
    # with a value function, see https://arxiv.org/pdf/1506.02438.pdf in
    # case an input line ends with a non-terminal timestep.
    "use_gae": True,
    # Whether to calculate cumulative rewards. Must be True.
    "postprocess_inputs": True,

    # === Training ===
    # Scaling of advantages in exponential terms.
    # When beta is 0.0, MARWIL is reduced to behavior cloning
    # (imitation learning); see bc.py algorithm in this same directory.
    "beta": 1.0,
    # Balancing value estimation loss and policy optimization loss.
    "vf_coeff": 1.0,
    # If specified, clip the global norm of gradients by this amount.
    "grad_clip": None,
    # Learning rate for Adam optimizer.
    "lr": 1e-4,
    # The squared moving avg. advantage norm (c^2) update rate
    # (1e-8 in the paper).
    "moving_average_sqd_adv_norm_update_rate": 1e-8,
    # Starting value for the squared moving avg. advantage norm (c^2).
    "moving_average_sqd_adv_norm_start": 100.0,
    # Number of (independent) timesteps pushed through the loss
    # each SGD round.
    "train_batch_size": 2000,
    # Size of the replay buffer in (single and independent) timesteps.
    # The buffer gets filled by reading from the input files line-by-line
    # and adding all timesteps on one line at once. We then sample
    # uniformly from the buffer (`train_batch_size` samples) for
    # each training step.
    "replay_buffer_size": 10000,
    # Number of steps to read before learning starts.
    "learning_starts": 0,

    # A coeff to encourage higher action distribution entropy for exploration.
    "bc_logstd_coeff": 0.0,

    # === Parallelism ===
    "num_workers": 0,
})
# __sphinx_doc_end__
# fmt: on


class MARWILTrainer(Trainer):
    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG

    @override(Trainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        # Call super's validation method.
        super().validate_config(config)

        if config["num_gpus"] > 1:
            raise ValueError("`num_gpus` > 1 not yet supported for MARWIL!")

        if config["postprocess_inputs"] is False and config["beta"] > 0.0:
            raise ValueError(
                "`postprocess_inputs` must be True for MARWIL (to "
                "calculate accum., discounted returns)!"
            )

    @override(Trainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
        if config["framework"] == "torch":
            from ray.rllib.algorithms.marwil.marwil_torch_policy import (
                MARWILTorchPolicy,
            )

            return MARWILTorchPolicy
        else:
            return MARWILTFPolicy

    @override(Trainer)
    def setup(self, config: PartialTrainerConfigDict):
        super().setup(config)
        # `training_iteration` implementation: Setup buffer in `setup`, not
        # in `execution_plan` (deprecated).
        if self.config["_disable_execution_plan_api"] is True:
            self.local_replay_buffer = MultiAgentReplayBuffer(
                learning_starts=self.config["learning_starts"],
                capacity=self.config["replay_buffer_size"],
                replay_batch_size=self.config["train_batch_size"],
                replay_sequence_length=1,
            )

    @override(Trainer)
    def training_iteration(self) -> ResultDict:
        # Collect SampleBatches from sample workers.
        batch = synchronous_parallel_sample(worker_set=self.workers)
        batch = batch.as_multi_agent()
        self._counters[NUM_AGENT_STEPS_SAMPLED] += batch.agent_steps()
        self._counters[NUM_ENV_STEPS_SAMPLED] += batch.env_steps()
        # Add batch to replay buffer.
        self.local_replay_buffer.add_batch(batch)

        # Pull batch from replay buffer and train on it.
        train_batch = self.local_replay_buffer.replay()
        # Train.
        if self.config["simple_optimizer"]:
            train_results = train_one_step(self, train_batch)
        else:
            train_results = multi_gpu_train_one_step(self, train_batch)

        # TODO: Move training steps counter update outside of `train_one_step()` method.
        # # Update train step counters.
        # self._counters[NUM_ENV_STEPS_TRAINED] += train_batch.env_steps()
        # self._counters[NUM_AGENT_STEPS_TRAINED] += train_batch.agent_steps()

        global_vars = {
            "timestep": self._counters[NUM_AGENT_STEPS_SAMPLED],
        }

        # Update weights - after learning on the local worker - on all remote
        # workers.
        if self.workers.remote_workers():
            with self._timers[WORKER_UPDATE_TIMER]:
                self.workers.sync_weights(global_vars=global_vars)

        # Update global vars on local worker as well.
        self.workers.local_worker().set_global_vars(global_vars)

        return train_results
