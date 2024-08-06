from typing import Callable, Optional, Type, Union

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.marwil.marwil_catalog import MARWILCatalog
from ray.rllib.algorithms.marwil.marwil_offline_prelearner import (
    MARWILOfflinePreLearner,
)
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.execution.rollout_ops import (
    synchronous_parallel_sample,
)
from ray.rllib.execution.train_ops import (
    multi_gpu_train_one_step,
    train_one_step,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    LEARNER_RESULTS,
    LEARNER_UPDATE_TIMER,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED_LIFETIME,
    NUM_MODULE_STEPS_TRAINED,
    NUM_MODULE_STEPS_TRAINED_LIFETIME,
    OFFLINE_SAMPLING_TIMER,
    SAMPLE_TIMER,
    SYNCH_WORKER_WEIGHTS_TIMER,
    TIMERS,
)
from ray.rllib.utils.typing import (
    EnvType,
    ResultDict,
    RLModuleSpecType,
)
from ray.tune.logger import Logger


class MARWILConfig(AlgorithmConfig):
    """Defines a configuration class from which a MARWIL Algorithm can be built.


    Example:
        >>> from ray.rllib.algorithms.marwil import MARWILConfig
        >>> # Run this from the ray directory root.
        >>> config = MARWILConfig()  # doctest: +SKIP
        >>> config = config.training(beta=1.0, lr=0.00001, gamma=0.99)  # doctest: +SKIP
        >>> config = config.offline_data(  # doctest: +SKIP
        ...     input_=["./rllib/tests/data/cartpole/large.json"])
        >>> print(config.to_dict()) # doctest: +SKIP
        ...
        >>> # Build an Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build()  # doctest: +SKIP
        >>> algo.train() # doctest: +SKIP

    Example:
        >>> from ray.rllib.algorithms.marwil import MARWILConfig
        >>> from ray import tune
        >>> config = MARWILConfig()
        >>> # Print out some default values.
        >>> print(config.beta)  # doctest: +SKIP
        >>> # Update the config object.
        >>> config.training(lr=tune.grid_search(  # doctest: +SKIP
        ...     [0.001, 0.0001]), beta=0.75)
        >>> # Set the config object's data path.
        >>> # Run this from the ray directory root.
        >>> config.offline_data( # doctest: +SKIP
        ...     input_=["./rllib/tests/data/cartpole/large.json"])
        >>> # Set the config object's env, used for evaluation.
        >>> config.environment(env="CartPole-v1")  # doctest: +SKIP
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.Tuner(  # doctest: +SKIP
        ...     "MARWIL",
        ...     param_space=config.to_dict(),
        ... ).fit()
    """

    def __init__(self, algo_class=None):
        """Initializes a MARWILConfig instance."""
        super().__init__(algo_class=algo_class or MARWIL)

        # fmt: off
        # __sphinx_doc_begin__
        # MARWIL specific settings:
        self.beta = 1.0
        self.bc_logstd_coeff = 0.0
        self.moving_average_sqd_adv_norm_update_rate = 1e-8
        self.moving_average_sqd_adv_norm_start = 100.0
        self.vf_coeff = 1.0
        self.model["vf_share_layers"] = False
        self.grad_clip = None

        # Override some of AlgorithmConfig's default values with MARWIL-specific values.

        # Define the `OfflinePreLearner` class for `MARWIL`.
        self.prelearner_class = MARWILOfflinePreLearner

        # You should override input_ to point to an offline dataset
        # (see algorithm.py and algorithm_config.py).
        # The dataset may have an arbitrary number of timesteps
        # (and even episodes) per line.
        # However, each line must only contain consecutive timesteps in
        # order for MARWIL to be able to calculate accumulated
        # discounted returns. It is ok, though, to have multiple episodes in
        # the same line.
        self.input_ = "sampler"
        self.postprocess_inputs = True
        self.lr = 1e-4
        self.lambda_ = 1.0
        self.train_batch_size = 2000
        # TODO (Artur): MARWIL should not need an exploration config as an offline
        #  algorithm. However, the current implementation of the CRR algorithm
        #  requires it. Investigate.
        self.exploration_config = {
            # The Exploration class to use. In the simplest case, this is the name
            # (str) of any class present in the `rllib.utils.exploration` package.
            # You can also provide the python class directly or the full location
            # of your class (e.g. "ray.rllib.utils.exploration.epsilon_greedy.
            # EpsilonGreedy").
            "type": "StochasticSampling",
            # Add constructor kwargs here (if any).
        }
        # __sphinx_doc_end__
        # fmt: on
        self._set_off_policy_estimation_methods = False

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        beta: Optional[float] = NotProvided,
        bc_logstd_coeff: Optional[float] = NotProvided,
        moving_average_sqd_adv_norm_update_rate: Optional[float] = NotProvided,
        moving_average_sqd_adv_norm_start: Optional[float] = NotProvided,
        vf_coeff: Optional[float] = NotProvided,
        grad_clip: Optional[float] = NotProvided,
        **kwargs,
    ) -> "MARWILConfig":
        """Sets the training related configuration.

        Args:
            beta: Scaling  of advantages in exponential terms. When beta is 0.0,
                MARWIL is reduced to behavior cloning (imitation learning);
                see bc.py algorithm in this same directory.
            bc_logstd_coeff: A coefficient to encourage higher action distribution
                entropy for exploration.
            moving_average_sqd_adv_norm_start: Starting value for the
                squared moving average advantage norm (c^2).
            vf_coeff: Balancing value estimation loss and policy optimization loss.
                moving_average_sqd_adv_norm_update_rate: Update rate for the
                squared moving average advantage norm (c^2).
            grad_clip: If specified, clip the global norm of gradients by this amount.

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)
        if beta is not NotProvided:
            self.beta = beta
        if bc_logstd_coeff is not NotProvided:
            self.bc_logstd_coeff = bc_logstd_coeff
        if moving_average_sqd_adv_norm_update_rate is not NotProvided:
            self.moving_average_sqd_adv_norm_update_rate = (
                moving_average_sqd_adv_norm_update_rate
            )
        if moving_average_sqd_adv_norm_start is not NotProvided:
            self.moving_average_sqd_adv_norm_start = moving_average_sqd_adv_norm_start
        if vf_coeff is not NotProvided:
            self.vf_coeff = vf_coeff
        if grad_clip is not NotProvided:
            self.grad_clip = grad_clip
        return self

    @override(AlgorithmConfig)
    def get_default_rl_module_spec(self) -> RLModuleSpecType:
        if self.framework_str == "torch":
            from ray.rllib.algorithms.marwil.torch.marwil_torch_rl_module import (
                MARWILTorchRLModule,
            )

            return RLModuleSpec(
                module_class=MARWILTorchRLModule,
                catalog_class=MARWILCatalog,
            )
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. "
                "Use either 'torch' or 'tf2'."
            )

    @override(AlgorithmConfig)
    def get_default_learner_class(self) -> Union[Type["Learner"], str]:
        if self.framework_str == "torch":
            from ray.rllib.algorithms.marwil.torch.marwil_torch_learner import (
                MARWILTorchLearner,
            )

            return MARWILTorchLearner
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. " "Use 'torch'."
            )

    @override(AlgorithmConfig)
    def evaluation(
        self,
        **kwargs,
    ) -> "MARWILConfig":
        """Sets the evaluation related configuration.
        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `evaluation()` method.
        super().evaluation(**kwargs)

        if "off_policy_estimation_methods" in kwargs:
            # User specified their OPE methods.
            self._set_off_policy_estimation_methods = True

        return self

    @override(AlgorithmConfig)
    def offline_data(self, **kwargs) -> "MARWILConfig":

        super().offline_data(**kwargs)

        # Check, if the passed in class incorporates the `OfflinePreLearner`
        # interface.
        if "prelearner_class" in kwargs:
            from ray.rllib.offline.offline_data import OfflinePreLearner

            if not issubclass(kwargs.get("prelearner_class"), OfflinePreLearner):
                raise ValueError(
                    f"`prelearner_class` {kwargs.get('prelearner_class')} is not a "
                    "subclass of `OfflinePreLearner`. Any class passed to "
                    "`prelearner_class` needs to implement the interface given by "
                    "`OfflinePreLearner`."
                )

        return self

    @override(AlgorithmConfig)
    def build(
        self,
        env: Optional[Union[str, EnvType]] = None,
        logger_creator: Optional[Callable[[], Logger]] = None,
    ) -> "Algorithm":
        if not self._set_off_policy_estimation_methods:
            deprecation_warning(
                old=r"MARWIL used to have off_policy_estimation_methods "
                "is and wis by default. This has"
                "changed to off_policy_estimation_methods: \{\}."
                "If you want to use an off-policy estimator, specify it in"
                ".evaluation(off_policy_estimation_methods=...)",
                error=False,
            )
        return super().build(env, logger_creator)

    @override(AlgorithmConfig)
    def validate(self) -> None:
        # Call super's validation method.
        super().validate()

        if self.beta < 0.0 or self.beta > 1.0:
            raise ValueError("`beta` must be within 0.0 and 1.0!")

        if self.postprocess_inputs is False and self.beta > 0.0:
            raise ValueError(
                "`postprocess_inputs` must be True for MARWIL (to "
                "calculate accum., discounted returns)! Try setting "
                "`config.offline_data(postprocess_inputs=True)`."
            )

        # Assert that for a local learner the number of iterations is 1. Note,
        # this is needed because we have no iterators, but instead a single
        # batch returned directly from the `OfflineData.sample` method.
        if self.num_learners == 0 and not self.dataset_num_iters_per_learner:
            self.dataset_num_iters_per_learner = 1

    @property
    def _model_auto_keys(self):
        return super()._model_auto_keys | {"beta": self.beta, "vf_share_layers": False}


class MARWIL(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return MARWILConfig()

    @classmethod
    @override(Algorithm)
    def get_default_policy_class(
        cls, config: AlgorithmConfig
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            from ray.rllib.algorithms.marwil.marwil_torch_policy import (
                MARWILTorchPolicy,
            )

            return MARWILTorchPolicy
        elif config["framework"] == "tf":
            from ray.rllib.algorithms.marwil.marwil_tf_policy import (
                MARWILTF1Policy,
            )

            return MARWILTF1Policy
        else:
            from ray.rllib.algorithms.marwil.marwil_tf_policy import MARWILTF2Policy

            return MARWILTF2Policy

    @override(Algorithm)
    def training_step(self) -> ResultDict:
        if self.config.enable_env_runner_and_connector_v2:
            return self._training_step_new_stack()
        elif self.config.enable_rl_module_and_learner:
            raise ValueError(
                "`enable_rl_module_and_learner=True`. Hybrid stack is not "
                "is not supported for MARWIL. Either use the old stack with "
                "`ModelV2` or the new stack with `RLModule`. You can enable "
                "the new stack by setting both, `enable_rl_module_and_learner` "
                "and `enable_env_runner_and_connector_v2` to `True`."
            )
        else:
            return self._training_step_old_stack()

    def _training_step_new_stack(self) -> ResultDict:
        """Implements training logic for the new stack

        Note, this includes so far training with the `OfflineData`
        class (multi-/single-learner setup) and evaluation on
        `EnvRunner`s. Note further, evaluation on the dataset itself
        using estimators is not implemented, yet.
        """
        # Implement logic using RLModule and Learner API.
        # TODO (simon): Take care of sampler metrics: right
        # now all rewards are `nan`, which possibly confuses
        # the user that sth. is not right, although it is as
        # we do not step the env.
        with self.metrics.log_time((TIMERS, OFFLINE_SAMPLING_TIMER)):
            # Sampling from offline data.
            batch = self.offline_data.sample(
                num_samples=self.config.train_batch_size_per_learner,
                num_shards=self.config.num_learners,
                return_iterator=True if self.config.num_learners > 1 else False,
            )

        with self.metrics.log_time((TIMERS, LEARNER_UPDATE_TIMER)):
            # Updating the policy.
            # TODO (simon, sven): Check, if we should execute directly s.th. like
            # update_from_iterator.
            learner_results = self.learner_group.update_from_batch(
                batch,
                minibatch_size=self.config.train_batch_size_per_learner,
                num_iters=self.config.dataset_num_iters_per_learner,
            )

            # Log training results.
            self.metrics.merge_and_log_n_dicts(learner_results, key=LEARNER_RESULTS)
            self.metrics.log_value(
                NUM_ENV_STEPS_TRAINED_LIFETIME,
                self.metrics.peek(
                    (LEARNER_RESULTS, ALL_MODULES, NUM_ENV_STEPS_TRAINED)
                ),
                reduce="sum",
            )
            self.metrics.log_dict(
                {
                    (LEARNER_RESULTS, mid, NUM_MODULE_STEPS_TRAINED_LIFETIME): (
                        stats[NUM_MODULE_STEPS_TRAINED]
                    )
                    for mid, stats in self.metrics.peek(LEARNER_RESULTS).items()
                },
                reduce="sum",
            )
        # Synchronize weights.
        # As the results contain for each policy the loss and in addition the
        # total loss over all policies is returned, this total loss has to be
        # removed.
        modules_to_update = set(learner_results[0].keys()) - {ALL_MODULES}

        # Update weights - after learning on the local worker -
        # on all remote workers.
        with self.metrics.log_time((TIMERS, SYNCH_WORKER_WEIGHTS_TIMER)):
            self.env_runner_group.sync_weights(
                # Sync weights from learner_group to all EnvRunners.
                from_worker_or_learner_group=self.learner_group,
                policies=modules_to_update,
                inference_only=True,
            )

        return self.metrics.reduce()

    def _training_step_old_stack(self) -> ResultDict:
        # Collect SampleBatches from sample workers.
        with self._timers[SAMPLE_TIMER]:
            train_batch = synchronous_parallel_sample(worker_set=self.env_runner_group)
        train_batch = train_batch.as_multi_agent(
            module_id=list(self.config.policies)[0]
        )
        self._counters[NUM_AGENT_STEPS_SAMPLED] += train_batch.agent_steps()
        self._counters[NUM_ENV_STEPS_SAMPLED] += train_batch.env_steps()

        # Train.
        if self.config.simple_optimizer:
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
        # workers (only those policies that were actually trained).
        if self.env_runner_group.remote_workers():
            with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
                self.env_runner_group.sync_weights(
                    policies=list(train_results.keys()), global_vars=global_vars
                )

        # Update global vars on local worker as well.
        self.env_runner.set_global_vars(global_vars)

        return train_results
