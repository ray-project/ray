from typing import Type, TYPE_CHECKING, Union

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.bc.bc_catalog import BCCatalog
from ray.rllib.algorithms.marwil.marwil import MARWIL, MARWILConfig
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    LEARNER_RESULTS,
    LEARNER_UPDATE_TIMER,
    OFFLINE_SAMPLING_TIMER,
    NUM_ENV_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED_LIFETIME,
    NUM_MODULE_STEPS_TRAINED,
    NUM_MODULE_STEPS_TRAINED_LIFETIME,
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
        if not self.config.enable_rl_module_and_learner:
            # Using ModelV2.
            return super().training_step()
        else:
            # Implement logic using RLModule and Learner API.
            # TODO (simon): Take care of sampler metrics: right
            # now all rewards are `nan`, which possibly confuses
            # the user that sth. is not right, although it is as
            # we do not step the env.
            with self.metrics.log_time((TIMERS, OFFLINE_SAMPLING_TIMER)):
                # Sampling from offline data.
                batch = self.offline_data.sample(
                    num_samples=self.config.train_batch_size,
                    num_shards=self.config.num_learners,
                    return_iterator=True if self.config.num_learners > 1 else False,
                )

            with self.metrics.log_time((TIMERS, LEARNER_UPDATE_TIMER)):
                # Updating the policy.
                # TODO (simon, sven): Check, if we should execute directly s.th. like
                # update_from_iterator.
                learner_results = self.learner_group.update_from_batch(batch)

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
                if self.workers.num_remote_workers() > 0:
                    self.workers.sync_weights(
                        from_worker_or_learner_group=self.learner_group,
                        policies=modules_to_update,
                        inference_only=True,
                    )
                # Then we must have a local worker.
                else:
                    weights = self.learner_group.get_weights(inference_only=True)
                    self.workers.local_worker().set_weights(weights)

            return self.metrics.reduce()
