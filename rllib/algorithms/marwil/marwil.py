from typing import Callable, Optional, Type, Union

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.connectors.learner import (
    AddObservationsFromEpisodesToBatch,
    AddOneTsToEpisodesAndTruncate,
    AddNextObservationsFromEpisodesToTrainBatch,
    GeneralAdvantageEstimation,
)
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.learner.training_data import TrainingData
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.execution.rollout_ops import (
    synchronous_parallel_sample,
)
from ray.rllib.execution.train_ops import (
    multi_gpu_train_one_step,
    train_one_step,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import OldAPIStack, override
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.metrics import (
    LEARNER_RESULTS,
    LEARNER_UPDATE_TIMER,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
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

    .. testcode::

        import gymnasium as gym
        import numpy as np

        from pathlib import Path
        from ray.rllib.algorithms.marwil import MARWILConfig

        # Get the base path (to ray/rllib)
        base_path = Path(__file__).parents[2]
        # Get the path to the data in rllib folder.
        data_path = base_path / "tests/data/cartpole/cartpole-v1_large"

        config = MARWILConfig()
        # Enable the new API stack.
        config.api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
        # Define the environment for which to learn a policy
        # from offline data.
        config.environment(
            observation_space=gym.spaces.Box(
                np.array([-4.8, -np.inf, -0.41887903, -np.inf]),
                np.array([4.8, np.inf, 0.41887903, np.inf]),
                shape=(4,),
                dtype=np.float32,
            ),
            action_space=gym.spaces.Discrete(2),
        )
        # Set the training parameters.
        config.training(
            beta=1.0,
            lr=1e-5,
            gamma=0.99,
            # We must define a train batch size for each
            # learner (here 1 local learner).
            train_batch_size_per_learner=2000,
        )
        # Define the data source for offline data.
        config.offline_data(
            input_=[data_path.as_posix()],
            # Run exactly one update per training iteration.
            dataset_num_iters_per_learner=1,
        )

        # Build an `Algorithm` object from the config and run 1 training
        # iteration.
        algo = config.build()
        algo.train()

    .. testcode::

        import gymnasium as gym
        import numpy as np

        from pathlib import Path
        from ray.rllib.algorithms.marwil import MARWILConfig
        from ray import tune

        # Get the base path (to ray/rllib)
        base_path = Path(__file__).parents[2]
        # Get the path to the data in rllib folder.
        data_path = base_path / "tests/data/cartpole/cartpole-v1_large"

        config = MARWILConfig()
        # Enable the new API stack.
        config.api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
        # Print out some default values
        print(f"beta: {config.beta}")
        # Update the config object.
        config.training(
            lr=tune.grid_search([1e-3, 1e-4]),
            beta=0.75,
            # We must define a train batch size for each
            # learner (here 1 local learner).
            train_batch_size_per_learner=2000,
        )
        # Set the config's data path.
        config.offline_data(
            input_=[data_path.as_posix()],
            # Set the number of updates to be run per learner
            # per training step.
            dataset_num_iters_per_learner=1,
        )
        # Set the config's environment for evalaution.
        config.environment(
            observation_space=gym.spaces.Box(
                np.array([-4.8, -np.inf, -0.41887903, -np.inf]),
                np.array([4.8, np.inf, 0.41887903, np.inf]),
                shape=(4,),
                dtype=np.float32,
            ),
            action_space=gym.spaces.Discrete(2),
        )
        # Set up a tuner to run the experiment.
        tuner = tune.Tuner(
            "MARWIL",
            param_space=config,
            run_config=tune.RunConfig(
                stop={"training_iteration": 1},
            ),
        )
        # Run the experiment.
        tuner.fit()
    """

    def __init__(self, algo_class=None):
        """Initializes a MARWILConfig instance."""
        self.exploration_config = {
            # The Exploration class to use. In the simplest case, this is the name
            # (str) of any class present in the `rllib.utils.exploration` package.
            # You can also provide the python class directly or the full location
            # of your class (e.g. "ray.rllib.utils.exploration.epsilon_greedy.
            # EpsilonGreedy").
            "type": "StochasticSampling",
            # Add constructor kwargs here (if any).
        }

        super().__init__(algo_class=algo_class or MARWIL)
        self._is_online = False
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

        # Materialize only the data in raw format, but not the mapped data b/c
        # MARWIL uses a connector to calculate values and therefore the module
        # needs to be updated frequently. This updating would not work if we
        # map the data once at the beginning.
        # TODO (simon, sven): The module is only updated when the OfflinePreLearner
        #   gets reinitiated, i.e. when the iterator gets reinitiated. This happens
        #   frequently enough with a small dataset, but with a big one this does not
        #   update often enough. We might need to put model weigths every couple of
        #   iterations into the object storage (maybe also connector states).
        self.materialize_data = True
        self.materialize_mapped_data = False
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
            moving_average_sqd_adv_norm_update_rate: The rate for updating the
                squared moving average advantage norm (c^2). A higher rate leads
                to faster updates of this moving avergage.
            moving_average_sqd_adv_norm_start: Starting value for the
                squared moving average advantage norm (c^2).
            vf_coeff: Balancing value estimation loss and policy optimization loss.
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
            from ray.rllib.algorithms.ppo.torch.default_ppo_torch_rl_module import (
                DefaultPPOTorchRLModule,
            )

            return RLModuleSpec(module_class=DefaultPPOTorchRLModule)
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. "
                "Use 'torch' instead."
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
                f"The framework {self.framework_str} is not supported. "
                "Use 'torch' instead."
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
                r"changed to off_policy_estimation_methods: \{\}."
                "If you want to use an off-policy estimator, specify it in"
                ".evaluation(off_policy_estimation_methods=...)",
                error=False,
            )
        return super().build(env, logger_creator)

    @override(AlgorithmConfig)
    def build_learner_connector(
        self,
        input_observation_space,
        input_action_space,
        device=None,
    ):
        pipeline = super().build_learner_connector(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            device=device,
        )

        # Before anything, add one ts to each episode (and record this in the loss
        # mask, so that the computations at this extra ts are not used to compute
        # the loss).
        pipeline.prepend(AddOneTsToEpisodesAndTruncate())

        # Prepend the "add-NEXT_OBS-from-episodes-to-train-batch" connector piece (right
        # after the corresponding "add-OBS-..." default piece).
        pipeline.insert_after(
            AddObservationsFromEpisodesToBatch,
            AddNextObservationsFromEpisodesToTrainBatch(),
        )

        # At the end of the pipeline (when the batch is already completed), add the
        # GAE connector, which performs a vf forward pass, then computes the GAE
        # computations, and puts the results of this (advantages, value targets)
        # directly back in the batch. This is then the batch used for
        # `forward_train` and `compute_losses`.
        pipeline.append(
            GeneralAdvantageEstimation(gamma=self.gamma, lambda_=self.lambda_)
        )

        return pipeline

    @override(AlgorithmConfig)
    def validate(self) -> None:
        # Call super's validation method.
        super().validate()

        if self.beta < 0.0 or self.beta > 1.0:
            self._value_error("`beta` must be within 0.0 and 1.0!")

        if self.postprocess_inputs is False and self.beta > 0.0:
            self._value_error(
                "`postprocess_inputs` must be True for MARWIL (to "
                "calculate accum., discounted returns)! Try setting "
                "`config.offline_data(postprocess_inputs=True)`."
            )

        # Assert that for a local learner the number of iterations is 1. Note,
        # this is needed because we have no iterators, but instead a single
        # batch returned directly from the `OfflineData.sample` method.
        if (
            self.num_learners == 0
            and not self.dataset_num_iters_per_learner
            and self.enable_rl_module_and_learner
        ):
            self._value_error(
                "When using a local Learner (`config.num_learners=0`), the number of "
                "iterations per learner (`dataset_num_iters_per_learner`) has to be "
                "defined! Set this hyperparameter through `config.offline_data("
                "dataset_num_iters_per_learner=...)`."
            )

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
    def training_step(self) -> None:
        """Implements training logic for the new stack

        Note, this includes so far training with the `OfflineData`
        class (multi-/single-learner setup) and evaluation on
        `EnvRunner`s. Note further, evaluation on the dataset itself
        using estimators is not implemented, yet.
        """
        # Old API stack (Policy, RolloutWorker, Connector).
        if not self.config.enable_env_runner_and_connector_v2:
            return self._training_step_old_api_stack()

        # TODO (simon): Take care of sampler metrics: right
        #  now all rewards are `nan`, which possibly confuses
        #  the user that sth. is not right, although it is as
        #  we do not step the env.
        with self.metrics.log_time((TIMERS, OFFLINE_SAMPLING_TIMER)):
            # If we should use an iterator in the learner(s). Note, in case of
            # multiple learners we must always return a list of iterators.
            return_iterator = (
                self.config.num_learners > 0
                or self.config.dataset_num_iters_per_learner != 1
            )

            # Sampling from offline data.
            batch_or_iterator = self.offline_data.sample(
                num_samples=self.config.train_batch_size_per_learner,
                num_shards=self.config.num_learners,
                # Return an iterator, if a `Learner` should update
                # multiple times per RLlib iteration.
                return_iterator=return_iterator,
            )
            if return_iterator:
                training_data = TrainingData(data_iterators=batch_or_iterator)
            else:
                training_data = TrainingData(batch=batch_or_iterator)

        with self.metrics.log_time((TIMERS, LEARNER_UPDATE_TIMER)):
            # Updating the policy.
            learner_results = self.learner_group.update(
                training_data=training_data,
                minibatch_size=self.config.train_batch_size_per_learner,
                num_iters=self.config.dataset_num_iters_per_learner,
                **self.offline_data.iter_batches_kwargs,
            )

            # Log training results.
            self.metrics.aggregate(learner_results, key=LEARNER_RESULTS)

    @OldAPIStack
    def _training_step_old_api_stack(self) -> ResultDict:
        """Implements training step for the old stack.

        Note, there is no hybrid stack anymore. If you need to use `RLModule`s,
        use the new api stack.
        """
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
        if self.env_runner_group.num_remote_env_runners() > 0:
            with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
                self.env_runner_group.sync_weights(
                    policies=list(train_results.keys()), global_vars=global_vars
                )

        # Update global vars on local worker as well.
        self.env_runner.set_global_vars(global_vars)

        return train_results
