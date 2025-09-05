import logging
from typing import Any, Dict, List, Optional, Type, Union, TYPE_CHECKING

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.execution.rollout_ops import (
    standardize_fields,
    synchronous_parallel_sample,
)
from ray.rllib.execution.train_ops import (
    train_one_step,
    multi_gpu_train_one_step,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import OldAPIStack, override
from ray.rllib.utils.deprecation import DEPRECATED_VALUE
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    ENV_RUNNER_SAMPLING_TIMER,
    LEARNER_RESULTS,
    LEARNER_UPDATE_TIMER,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    SYNCH_WORKER_WEIGHTS_TIMER,
    SAMPLE_TIMER,
    TIMERS,
    ALL_MODULES,
)
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.typing import ResultDict
from ray.util.debug import log_once

from ray.rllib.algorithms.ppo.ppo import PPOConfig, PPO

if TYPE_CHECKING:
    from ray.rllib.core.learner.learner import Learner

from ray.rllib.examples.algorithms.mappo.torch.mappo_torch_learner import MAPPOTorchLearner
from ray.rllib.examples.algorithms.mappo.torch.default_mappo_torch_rl_module import DefaultMAPPOTorchRLModule

logger = logging.getLogger(__name__)

LEARNER_RESULTS_KL_KEY = "mean_kl_loss"
LEARNER_RESULTS_CURR_KL_COEFF_KEY = "curr_kl_coeff"
LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY = "curr_entropy_coeff"


class MAPPOConfig(PPOConfig): # AlgorithmConfig -> PPOConfig -> MAPPO
    """Defines a configuration class from which a MAPPO Algorithm can be built.
    """

    def __init__(self, algo_class=None):
        """Initializes a MAPPOConfig instance."""
        self.exploration_config = {
            # The Exploration class to use. In the simplest case, this is the name
            # (str) of any class present in the `rllib.utils.exploration` package.
            # You can also provide the python class directly or the full location
            # of your class (e.g. "ray.rllib.utils.exploration.epsilon_greedy.
            # EpsilonGreedy").
            "type": "StochasticSampling",
            # Add constructor kwargs here (if any).
        }

        super().__init__(algo_class=algo_class or PPO)

        # fmt: off
        # __sphinx_doc_begin__
        self.lr = 5e-5
        self.rollout_fragment_length = "auto"
        self.train_batch_size = 4000

        # PPO specific settings:
        self.num_epochs = 30
        self.minibatch_size = 128
        self.shuffle_batch_per_epoch = True
        self.lambda_ = 1.0
        self.use_kl_loss = True
        self.kl_coeff = 0.2
        self.kl_target = 0.01
        self.entropy_coeff = 0.0
        self.clip_param = 0.3
        self.grad_clip = None

        # Override some of AlgorithmConfig's default values with PPO-specific values.
        self.num_env_runners = 2
        # __sphinx_doc_end__
        # fmt: on

        self.entropy_coeff_schedule = None  # OldAPIStack
        self.lr_schedule = None  # OldAPIStack

        # Deprecated keys.
        self.sgd_minibatch_size = DEPRECATED_VALUE

    @override(AlgorithmConfig)
    def get_default_rl_module_spec(self) -> RLModuleSpec:
        if self.framework_str == "torch":
            return RLModuleSpec(module_class=DefaultMAPPOTorchRLModule)
        raise NotImplementedError()

    @override(AlgorithmConfig)
    def get_default_learner_class(self) -> Union[Type["Learner"], str]:
        if self.framework_str == "torch":
            return MAPPOTorchLearner
        raise NotImplementedError()

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        lambda_: Optional[float] = NotProvided,
        use_kl_loss: Optional[bool] = NotProvided,
        kl_coeff: Optional[float] = NotProvided,
        kl_target: Optional[float] = NotProvided,
        entropy_coeff: Optional[float] = NotProvided,
        entropy_coeff_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        clip_param: Optional[float] = NotProvided,
        grad_clip: Optional[float] = NotProvided,
        # OldAPIStack
        lr_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        **kwargs,
    ) -> "PPOConfig":
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)
        if lambda_ is not NotProvided:
            self.lambda_ = lambda_
        if use_kl_loss is not NotProvided:
            self.use_kl_loss = use_kl_loss
        if kl_coeff is not NotProvided:
            self.kl_coeff = kl_coeff
        if kl_target is not NotProvided:
            self.kl_target = kl_target
        if entropy_coeff is not NotProvided:
            self.entropy_coeff = entropy_coeff
        if clip_param is not NotProvided:
            self.clip_param = clip_param
        if grad_clip is not NotProvided:
            self.grad_clip = grad_clip

        # TODO (sven): Remove these once new API stack is only option for PPO.
        if lr_schedule is not NotProvided:
            self.lr_schedule = lr_schedule
        if entropy_coeff_schedule is not NotProvided:
            self.entropy_coeff_schedule = entropy_coeff_schedule

        return self

    @override(AlgorithmConfig)
    def validate(self) -> None:
        # Call super's validation method.
        super().validate()

        # Synchronous sampling, on-policy/PPO algos -> Check mismatches between
        # `rollout_fragment_length` and `train_batch_size_per_learner` to avoid user
        # confusion.
        # TODO (sven): Make rollout_fragment_length a property and create a private
        #  attribute to store (possibly) user provided value (or "auto") in. Deprecate
        #  `self.get_rollout_fragment_length()`.
        self.validate_train_batch_size_vs_rollout_fragment_length()

        # SGD minibatch size must be smaller than train_batch_size (b/c
        # we subsample a batch of `minibatch_size` from the train-batch for
        # each `num_epochs`).
        if (
            not self.enable_rl_module_and_learner
            and self.minibatch_size > self.train_batch_size
        ):
            self._value_error(
                f"`minibatch_size` ({self.minibatch_size}) must be <= "
                f"`train_batch_size` ({self.train_batch_size}). In PPO, the train batch"
                f" will be split into {self.minibatch_size} chunks, each of which "
                f"is iterated over (used for updating the policy) {self.num_epochs} "
                "times."
            )
        elif self.enable_rl_module_and_learner:
            mbs = self.minibatch_size
            tbs = self.train_batch_size_per_learner or self.train_batch_size
            if isinstance(mbs, int) and isinstance(tbs, int) and mbs > tbs:
                self._value_error(
                    f"`minibatch_size` ({mbs}) must be <= "
                    f"`train_batch_size_per_learner` ({tbs}). In PPO, the train batch"
                    f" will be split into {mbs} chunks, each of which is iterated over "
                    f"(used for updating the policy) {self.num_epochs} times."
                )

        # Episodes may only be truncated (and passed into PPO's
        # `postprocessing_fn`), iff generalized advantage estimation is used
        # (value function estimate at end of truncated episode to estimate
        # remaining value).
        if (
            not self.in_evaluation
            and self.batch_mode == "truncate_episodes"
            and not self.use_gae
        ):
            self._value_error(
                "Episode truncation is not supported without a value "
                "function (to estimate the return at the end of the truncated"
                " trajectory). Consider setting "
                "batch_mode=complete_episodes."
            )

        # New API stack checks.
        if self.enable_rl_module_and_learner:
            # `lr_schedule` checking.
            if self.lr_schedule is not None:
                self._value_error(
                    "`lr_schedule` is deprecated and must be None! Use the "
                    "`lr` setting to setup a schedule."
                )
            if self.entropy_coeff_schedule is not None:
                self._value_error(
                    "`entropy_coeff_schedule` is deprecated and must be None! Use the "
                    "`entropy_coeff` setting to setup a schedule."
                )
            Scheduler.validate(
                fixed_value_or_schedule=self.entropy_coeff,
                setting_name="entropy_coeff",
                description="entropy coefficient",
            )
        if isinstance(self.entropy_coeff, float) and self.entropy_coeff < 0.0:
            self._value_error("`entropy_coeff` must be >= 0.0")

    @property
    @override(AlgorithmConfig)
    def _model_config_auto_includes(self) -> Dict[str, Any]:
        return super()._model_config_auto_includes | {}