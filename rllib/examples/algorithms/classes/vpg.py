from ray.rllib.algorithms import Algorithm, AlgorithmConfig
from ray.rllib.core import ALL_MODULES
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.learners.classes.vpg_learner import VPGTorchLearner
from ray.rllib.examples.rl_modules.classes.vpg_rlm import VPGTorchRLModule
from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    ENV_RUNNER_SAMPLING_TIMER,
    LEARNER_RESULTS,
    LEARNER_UPDATE_TIMER,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    SYNCH_WORKER_WEIGHTS_TIMER,
    TIMERS,
)
from ray.rllib.utils.typing import ResultDict


class VPGConfig(AlgorithmConfig):
    # A test setting to activate metrics on mean weights.
    report_mean_weights: bool = True

    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or VPG)

        # VPG specific overrides.
        self.num_env_runners = 1

        # Switch on the new API stack by default.
        self.api_stack(
            enable_env_runner_and_connector_v2=True,
            enable_rl_module_and_learner=True,
        )

    @override(AlgorithmConfig)
    def get_default_rl_module_spec(self):
        if self.framework_str == "torch":
            spec = RLModuleSpec(
                module_class=VPGTorchRLModule,
                model_config={"hidden_dim": 64},
            )
        else:
            raise ValueError(f"Unsupported framework: {self.framework_str}")

        #if self.is_multi_agent():
        #    # TODO (sven): Make this more multi-agent for example with policy ids
        #    #  "p0" and "p1".
        #    return MultiRLModuleSpec(
        #        multi_rl_module_class=MultiRLModule,
        #        module_specs={DEFAULT_MODULE_ID: spec},
        #    )
        #else:
        return spec

    @override(AlgorithmConfig)
    def get_default_learner_class(self):
        if self.framework_str == "torch":
            return VPGTorchLearner
        else:
            raise ValueError(f"Unsupported framework: {self.framework_str}")


class VPG(Algorithm):
    @override(Algorithm)
    def training_step(self) -> ResultDict:

        with self.metrics.log_time((TIMERS, ENV_RUNNER_SAMPLING_TIMER)):
            episodes, env_runner_results = synchronous_parallel_sample(
                worker_set=self.env_runner_group,
                max_env_steps=self.config.total_train_batch_size,
                sample_timeout_s=self.config.sample_timeout_s,
                _uses_new_env_runners=True,
                _return_metrics=True,
            )
        self.metrics.merge_and_log_n_dicts(env_runner_results, key=ENV_RUNNER_RESULTS)

        with self.metrics.log_time((TIMERS, LEARNER_UPDATE_TIMER)):
            learner_results = self.learner_group.update_from_episodes(
                episodes=episodes,
                timesteps={
                    NUM_ENV_STEPS_SAMPLED_LIFETIME: (
                        self.metrics.peek(NUM_ENV_STEPS_SAMPLED_LIFETIME)
                    ),
                },
            )
        self.metrics.log_dict(learner_results, key=LEARNER_RESULTS)

        with self.metrics.log_time((TIMERS, SYNCH_WORKER_WEIGHTS_TIMER)):
            self.env_runner_group.sync_weights(
                from_worker_or_learner_group=self.learner_group,
                policies=list(set(learner_results.keys()) - {ALL_MODULES}),
                inference_only=True,
            )

        return self.metrics.reduce()

