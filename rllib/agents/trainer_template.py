import logging
from typing import Callable, Iterable, List, Optional, Type

from ray.rllib.agents.trainer import Trainer, COMMON_CONFIG
from ray.rllib.env.env_context import EnvContext
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.rollout_ops import ParallelRollouts, ConcatBatches
from ray.rllib.execution.train_ops import TrainOneStep, TrainTFMultiGPU
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.policy import Policy
from ray.rllib.utils import add_mixins
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.typing import EnvConfigDict, EnvType, ResultDict, \
    TrainerConfigDict

logger = logging.getLogger(__name__)


def default_execution_plan(workers: WorkerSet, config: TrainerConfigDict):
    # Collects experiences in parallel from multiple RolloutWorker actors.
    rollouts = ParallelRollouts(workers, mode="bulk_sync")

    # Combine experiences batches until we hit `train_batch_size` in size.
    # Then, train the policy on those experiences and update the workers.
    train_op = rollouts.combine(
        ConcatBatches(
            min_batch_size=config["train_batch_size"],
            count_steps_by=config["multiagent"]["count_steps_by"],
        ))

    if config.get("simple_optimizer") is True:
        train_op = train_op.for_each(TrainOneStep(workers))
    else:
        train_op = train_op.for_each(
            TrainTFMultiGPU(
                workers=workers,
                sgd_minibatch_size=config.get("sgd_minibatch_size",
                                              config["train_batch_size"]),
                num_sgd_iter=config.get("num_sgd_iter", 1),
                num_gpus=config["num_gpus"],
                shuffle_sequences=config.get("shuffle_sequences", False),
                _fake_gpus=config["_fake_gpus"],
                framework=config["framework"]))

    # Add on the standard episode reward, etc. metrics reporting. This returns
    # a LocalIterator[metrics_dict] representing metrics for each train step.
    return StandardMetricsReporting(train_op, workers, config)


@DeveloperAPI
def build_trainer(
        name: str,
        *,
        default_config: Optional[TrainerConfigDict] = None,
        validate_config: Optional[Callable[[TrainerConfigDict], None]] = None,
        default_policy: Optional[Type[Policy]] = None,
        get_policy_class: Optional[Callable[[TrainerConfigDict], Optional[Type[
            Policy]]]] = None,
        validate_env: Optional[Callable[[EnvType, EnvContext], None]] = None,
        before_init: Optional[Callable[[Trainer], None]] = None,
        after_init: Optional[Callable[[Trainer], None]] = None,
        before_evaluate_fn: Optional[Callable[[Trainer], None]] = None,
        mixins: Optional[List[type]] = None,
        execution_plan: Optional[Callable[[
            WorkerSet, TrainerConfigDict
        ], Iterable[ResultDict]]] = default_execution_plan) -> Type[Trainer]:
    """Helper function for defining a custom trainer.

    Functions will be run in this order to initialize the trainer:
        1. Config setup: validate_config, get_policy
        2. Worker setup: before_init, execution_plan
        3. Post setup: after_init

    Args:
        name (str): name of the trainer (e.g., "PPO")
        default_config (Optional[TrainerConfigDict]): The default config dict
            of the algorithm, otherwise uses the Trainer default config.
        validate_config (Optional[Callable[[TrainerConfigDict], None]]):
            Optional callable that takes the config to check for correctness.
            It may mutate the config as needed.
        default_policy (Optional[Type[Policy]]): The default Policy class to
            use if `get_policy_class` returns None.
        get_policy_class (Optional[Callable[
            TrainerConfigDict, Optional[Type[Policy]]]]): Optional callable
            that takes a config and returns the policy class or None. If None
            is returned, will use `default_policy` (which must be provided
            then).
        validate_env (Optional[Callable[[EnvType, EnvContext], None]]):
            Optional callable to validate the generated environment (only
            on worker=0).
        before_init (Optional[Callable[[Trainer], None]]): Optional callable to
            run before anything is constructed inside Trainer (Workers with
            Policies, execution plan, etc..). Takes the Trainer instance as
            argument.
        after_init (Optional[Callable[[Trainer], None]]): Optional callable to
            run at the end of trainer init (after all Workers and the exec.
            plan have been constructed). Takes the Trainer instance as
            argument.
        before_evaluate_fn (Optional[Callable[[Trainer], None]]): Callback to
            run before evaluation. This takes the trainer instance as argument.
        mixins (list): list of any class mixins for the returned trainer class.
            These mixins will be applied in order and will have higher
            precedence than the Trainer class.
        execution_plan (Optional[Callable[[WorkerSet, TrainerConfigDict],
            Iterable[ResultDict]]]): Optional callable that sets up the
            distributed execution workflow.

    Returns:
        Type[Trainer]: A Trainer sub-class configured by the specified args.
    """

    original_kwargs = locals().copy()
    base = add_mixins(Trainer, mixins)

    class trainer_cls(base):
        _name = name
        _default_config = default_config or COMMON_CONFIG
        _policy_class = default_policy

        def __init__(self, config=None, env=None, logger_creator=None):
            Trainer.__init__(self, config, env, logger_creator)

        def _init(self, config: TrainerConfigDict,
                  env_creator: Callable[[EnvConfigDict], EnvType]):
            # Validate config via custom validation function.
            if validate_config:
                validate_config(config)

            # No `get_policy_class` function.
            if get_policy_class is None:
                # Default_policy must be provided (unless in multi-agent mode,
                # where each policy can have its own default policy class.
                if not config["multiagent"]["policies"]:
                    assert default_policy is not None
                self._policy_class = default_policy
            # Query the function for a class to use.
            else:
                self._policy_class = get_policy_class(config)
                # If None returned, use default policy (must be provided).
                if self._policy_class is None:
                    assert default_policy is not None
                    self._policy_class = default_policy

            if before_init:
                before_init(self)

            # Creating all workers (excluding evaluation workers).
            self.workers = self._make_workers(
                env_creator=env_creator,
                validate_env=validate_env,
                policy_class=self._policy_class,
                config=config,
                num_workers=self.config["num_workers"])
            self.execution_plan = execution_plan
            self.train_exec_impl = execution_plan(self.workers, config)

            if after_init:
                after_init(self)

        @override(Trainer)
        def step(self):
            res = next(self.train_exec_impl)

            # self._iteration gets incremented after this function returns,
            # meaning that e. g. the first time this function is called,
            # self._iteration will be 0. We check `self._iteration+1` in the
            # if-statement below to reflect that the first training iteration
            # is already over.
            if (self.config["evaluation_interval"] and (self._iteration + 1) %
                    self.config["evaluation_interval"] == 0):
                evaluation_metrics = self._evaluate()
                assert isinstance(evaluation_metrics, dict), \
                    "_evaluate() needs to return a dict."
                res.update(evaluation_metrics)
            return res

        @override(Trainer)
        def _before_evaluate(self):
            if before_evaluate_fn:
                before_evaluate_fn(self)

        @override(Trainer)
        def __getstate__(self):
            state = Trainer.__getstate__(self)
            state["train_exec_impl"] = (
                self.train_exec_impl.shared_metrics.get().save())
            return state

        @override(Trainer)
        def __setstate__(self, state):
            Trainer.__setstate__(self, state)
            self.train_exec_impl.shared_metrics.get().restore(
                state["train_exec_impl"])

        @staticmethod
        @override(Trainer)
        def with_updates(**overrides) -> Type[Trainer]:
            """Build a copy of this trainer class with the specified overrides.

            Keyword Args:
                overrides (dict): use this to override any of the arguments
                    originally passed to build_trainer() for this policy.

            Returns:
                Type[Trainer]: A the Trainer sub-class using `original_kwargs`
                    and `overrides`.

            Examples:
                >>> MyClass = SomeOtherClass.with_updates({"name": "Mine"})
                >>> issubclass(MyClass, SomeOtherClass)
                ... False
                >>> issubclass(MyClass, Trainer)
                ... True
            """
            return build_trainer(**dict(original_kwargs, **overrides))

    trainer_cls.__name__ = name
    trainer_cls.__qualname__ = name
    return trainer_cls
