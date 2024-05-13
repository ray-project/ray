from typing import Type, TYPE_CHECKING, Union

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.bc.bc_catalog import BCCatalog
from ray.rllib.algorithms.marwil.marwil import MARWIL, MARWILConfig
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    SAMPLE_TIMER,
    SYNCH_WORKER_WEIGHTS_TIMER,
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
        # Set RLModule as default.
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
                # here and just use the already distributed `dataset`
                # for sampling. Only in online evaluation
                # `RolloutWorker/EnvRunner` should be used.
                if self.config.count_steps_by == "agent_steps":
                    train_batch = synchronous_parallel_sample(
                        worker_set=self.workers,
                        max_agent_steps=self.config.train_batch_size,
                    )
                else:
                    train_batch = synchronous_parallel_sample(
                        worker_set=self.workers,
                        max_env_steps=self.config.train_batch_size,
                    )

                train_batch = train_batch.as_multi_agent()
                self._counters[NUM_AGENT_STEPS_SAMPLED] += train_batch.agent_steps()
                self._counters[NUM_ENV_STEPS_SAMPLED] += train_batch.env_steps()

            # Updating the policy.
            train_results = self.learner_group.update_from_batch(batch=train_batch)

            # Synchronize weights.
            # As the results contain for each policy the loss and in addition the
            # total loss over all policies is returned, this total loss has to be
            # removed.
            policies_to_update = set(train_results.keys()) - {ALL_MODULES}

            global_vars = {
                "timestep": self._counters[NUM_AGENT_STEPS_SAMPLED],
            }

            with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
                if self.workers.num_remote_workers() > 0:
                    self.workers.sync_weights(
                        from_worker_or_learner_group=self.learner_group,
                        policies=policies_to_update,
                        global_vars=global_vars,
                    )
                # Get weights from Learner to local worker.
                else:
                    self.workers.local_worker().set_weights(
                        self.learner_group.get_weights()
                    )

            return train_results
