from typing import Type, TYPE_CHECKING, Union

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.bc.bc_catalog import BCCatalog
from ray.rllib.algorithms.marwil.marwil import MARWIL, MARWILConfig
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    LEARNER_RESULTS,
    LEARNER_UPDATE_TIMER,
    OFFLINE_SAMPLING_TIMER,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED_LIFETIME,
    NUM_MODULE_STEPS_TRAINED,
    NUM_MODULE_STEPS_TRAINED_LIFETIME,
    SAMPLE_TIMER,
    SYNCH_WORKER_WEIGHTS_TIMER,
    TIMERS,
)
from ray.rllib.utils.typing import RLModuleSpec, ResultDict

if TYPE_CHECKING:
    from ray.rllib.core.learner import Learner


class BCConfig(MARWILConfig):
    """Defines a configuration class from which a new BC Algorithm can be built

    .. testcode::
        :skipif: True

        from ray.rllib.algorithms.bc import BCConfig
        # Run this from the ray directory root.
        config = BCConfig().training(lr=0.00001, gamma=0.99)
        config = config.offline_data(
            input_="./rllib/tests/data/cartpole/large.json")

        # Build an Algorithm object from the config and run 1 training iteration.
        algo = config.build()
        algo.train()

    .. testcode::
        :skipif: True

        from ray.rllib.algorithms.bc import BCConfig
        from ray import tune
        config = BCConfig()
        # Print out some default values.
        print(config.beta)
        # Update the config object.
        config.training(
            lr=tune.grid_search([0.001, 0.0001]), beta=0.75
        )
        # Set the config object's data path.
        # Run this from the ray directory root.
        config.offline_data(
            input_="./rllib/tests/data/cartpole/large.json"
        )
        # Set the config object's env, used for evaluation.
        config.environment(env="CartPole-v1")
        # Use to_dict() to get the old-style python config dict
        # when running with tune.
        tune.Tuner(
            "BC",
            param_space=config.to_dict(),
        ).fit()
    """

    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or BC)

        # fmt: off
        # __sphinx_doc_begin__
        # No need to calculate advantages (or do anything else with the rewards).
        self.beta = 0.0
        # Advantages (calculated during postprocessing)
        # not important for behavioral cloning.
        self.postprocess_inputs = False
        # Set RLModule as default if the `EnvRUnner`'s are used.
        if self.enable_env_runner_and_connector_v2:
            self.api_stack(enable_rl_module_and_learner=True)

        # __sphinx_doc_end__
        # fmt: on

    @override(AlgorithmConfig)
    def get_default_rl_module_spec(self) -> RLModuleSpec:
        if self.framework_str == "torch":
            from ray.rllib.algorithms.bc.torch.bc_torch_rl_module import BCTorchRLModule

            return SingleAgentRLModuleSpec(
                module_class=BCTorchRLModule,
                catalog_class=BCCatalog,
            )
        elif self.framework_str == "tf2":
            from ray.rllib.algorithms.bc.tf.bc_tf_rl_module import BCTfRLModule

            return SingleAgentRLModuleSpec(
                module_class=BCTfRLModule,
                catalog_class=BCCatalog,
            )
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. "
                "Use either 'torch' or 'tf2'."
            )

    @override(AlgorithmConfig)
    def get_default_learner_class(self) -> Union[Type["Learner"], str]:
        if self.framework_str == "torch":
            from ray.rllib.algorithms.bc.torch.bc_torch_learner import BCTorchLearner

            return BCTorchLearner
        elif self.framework_str == "tf2":
            from ray.rllib.algorithms.bc.tf.bc_tf_learner import BCTfLearner

            return BCTfLearner
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. "
                "Use either 'torch' or 'tf2'."
            )

    @override(MARWILConfig)
    def validate(self) -> None:
        # Call super's validation method.
        super().validate()

        if self.beta != 0.0:
            raise ValueError("For behavioral cloning, `beta` parameter must be 0.0!")


class BC(MARWIL):
    """Behavioral Cloning (derived from MARWIL).

    Simply uses MARWIL with beta force-set to 0.0.
    """

    @classmethod
    @override(MARWIL)
    def get_default_config(cls) -> AlgorithmConfig:
        return BCConfig()

    @override(MARWIL)
    def training_step(self) -> ResultDict:
        # Check, which stack is run.
        if self.config.enable_env_runner_and_connector_v2:
            # Using `EnvRunner`s, `OfflineData` and `RLModule`s.
            return self._training_step_new_stack()
        elif self.config.enable_rl_module_and_learner:
            # Using `RLModule`s, but `RolloutWorker`s.
            return self._training_step_hybrid_stack()
        else:
            # Using ModelV2 and `RolloutWorker`s.
            return super().training_step()

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
                **self.offline_data.iter_batches_kwargs
                if self.config.num_learners > 1
                else {},
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

    def _training_step_hybrid_stack(self) -> ResultDict:
        """Implements training logic for the hybrid stack.

        Note, the hybrid stack cannot fall back on MARWIL b/c MARWIL
        is still on the old stack. Instead it needs to use `RolloutWorkers`
        for evaluation and the `RLModule`s for inference and training.
        Specifically it cannot use the new `OfflineData` class for
        training.
        """
        # Implement logic using RLModule and Learner API.
        # TODO (sven): Remove RolloutWorkers/EnvRunners for
        # datasets. Use RolloutWorker/EnvRunner only for
        # env stepping.
        # TODO (simon): Take care of sampler metrics: right
        # now all rewards are `nan`, which possibly confuses
        # the user that sth. is not right, although it is as
        # we do not step the env.
        with self._timers[SAMPLE_TIMER]:
            # Sampling from offline data.
            # TODO (simon): We have to remove the `RolloutWorker`
            #  here and just use the already distributed `dataset`
            #  for sampling. Only in online evaluation
            #  `RolloutWorker/EnvRunner` should be used.
            if self.config.count_steps_by == "agent_steps":
                train_batch = synchronous_parallel_sample(
                    worker_set=self.env_runner_group,
                    max_agent_steps=self.config.train_batch_size,
                    sample_timeout_s=self.config.sample_timeout_s,
                )
            else:
                train_batch = synchronous_parallel_sample(
                    worker_set=self.env_runner_group,
                    max_env_steps=self.config.train_batch_size,
                    sample_timeout_s=self.config.sample_timeout_s,
                )

            # TODO (sven): Use metrics API as soon as we moved to new API stack
            #  (from currently hybrid stack).
            # self.metrics.log_dict(
            #    {
            #        NUM_AGENT_STEPS_SAMPLED_LIFETIME: len(train_batch),
            #        NUM_ENV_STEPS_SAMPLED_LIFETIME: len(train_batch),
            #    },
            #    reduce="sum",
            # )
            self._counters[NUM_AGENT_STEPS_SAMPLED] += len(train_batch)
            self._counters[NUM_ENV_STEPS_SAMPLED] += len(train_batch)

        # Updating the policy.
        train_results = self.learner_group.update_from_batch(
            batch=train_batch.as_multi_agent(module_id=list(self.config.policies)[0])
        )
        # TODO (sven): Use metrics API as soon as we moved to new API stack
        #  (from currently hybrid stack).
        # self.metrics.log_dict(
        #    {
        #        NUM_AGENT_STEPS_TRAINED_LIFETIME: len(train_batch),
        #        NUM_ENV_STEPS_TRAINED_LIFETIME: len(train_batch),
        #    },
        #    reduce="sum",
        # )
        self._counters[NUM_AGENT_STEPS_TRAINED] += len(train_batch)
        self._counters[NUM_ENV_STEPS_TRAINED] += len(train_batch)

        # Synchronize weights.
        # As the results contain for each policy the loss and in addition the
        # total loss over all policies is returned, this total loss has to be
        # removed.
        policies_to_update = set(train_results.keys()) - {ALL_MODULES}

        # with self.metrics.log_time((TIMERS, SYNCH_WORKER_WEIGHTS_TIMER)):
        with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
            if self.env_runner_group.num_remote_workers() > 0:
                self.env_runner_group.sync_weights(
                    from_worker_or_learner_group=self.learner_group,
                    policies=policies_to_update,
                )
            # Get weights from Learner to local worker.
            else:
                self.env_runner.set_weights(self.learner_group.get_weights())

        # TODO (sven): Use metrics API as soon as we moved to new API stack
        #  (from currently hybrid stack).
        return train_results
