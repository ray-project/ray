from collections import defaultdict
import logging
from typing import Callable, Dict, Iterable, List, Optional, Type, Union

from ray.rllib.agents.trainer import Trainer, COMMON_CONFIG
from ray.rllib.env.env_context import EnvContext
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.rollout_ops import ParallelRollouts, ConcatBatches
from ray.rllib.execution.train_ops import TrainOneStep, MultiGPUTrainOneStep
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.policy import Policy
from ray.rllib.utils import add_mixins, force_list
from ray.rllib.utils.annotations import override, Deprecated, DeveloperAPI
import ray.rllib.utils.events.events as events
from ray.rllib.utils.deprecation import DEPRECATED_VALUE
from ray.rllib.utils.typing import EnvConfigDict, EnvType, EventName, \
    ResultDict, TrainerConfigDict

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
            MultiGPUTrainOneStep(
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
def build_trainer_class(
        name: str,
        *,
        default_config: Optional[TrainerConfigDict] = None,
        allow_unknown_configs: bool = False,
        allow_unknown_subkeys: Optional[List[str]] = None,
        override_all_subkeys_if_type_changes: Optional[List[str]] = None,
        default_policy: Optional[Type[Policy]] = None,
        mixins: Optional[List[type]] = None,
        event_subscriptions: Optional[Dict[EventName, Union[List[Callable], Callable]]] = None,

        # OBSOLETE:
        validate_config: Optional[Callable[[TrainerConfigDict], None]] = DEPRECATED_VALUE,
        get_policy_class: Optional[Callable[[TrainerConfigDict], Optional[Type[
            Policy]]]] = DEPRECATED_VALUE,
        validate_env: Optional[Callable[[EnvType, EnvContext], None]] = DEPRECATED_VALUE,
        before_init: Optional[Callable[[Trainer], None]] = DEPRECATED_VALUE,
        after_init: Optional[Callable[[Trainer], None]] = DEPRECATED_VALUE,
        execution_plan: Optional[
            Callable[[WorkerSet, TrainerConfigDict], Iterable[ResultDict]]
        ] = DEPRECATED_VALUE,
        before_evaluate_fn: Optional[Callable[[Trainer], None]] = DEPRECATED_VALUE,
) -> Type[Trainer]:
    """Helper function for defining a custom trainer.

    Functions will be run in this order to initialize the trainer:
        1. Config setup: validate_config, get_policy
        2. Worker setup: trigger_event("before_make_workers"), execution_plan
        3. Post setup: trigger_event("after_make_workers")

    Args:
        name (str): name of the trainer (e.g., "PPO")
        default_config (Optional[TrainerConfigDict]): The default config dict
            of the algorithm, otherwise uses the Trainer default config.
        default_policy (Optional[Type[Policy]]): The default Policy class to
            use if `get_policy_class` returns None.
        mixins (list): list of any class mixins for the returned trainer class.
            These mixins will be applied in order and will have higher
            precedence than the Trainer class.
        allow_unknown_configs (bool): Whether to allow unknown top-level config
            keys.
        allow_unknown_subkeys (Optional[List[str]]): List of top-level keys
            with value=dict, for which new sub-keys are allowed to be added to
            the value dict. Appends to Trainer class defaults.
        override_all_subkeys_if_type_changes (Optional[List[str]]): List of top
            level keys with value=dict, for which we always override the entire
            value (dict), iff the "type" key in that value dict changes.
            Appends to Trainer class defaults.

        #Deprecated:
        execution_plan (Optional[Callable[[WorkerSet, TrainerConfigDict],
            Iterable[ResultDict]]]): Optional callable that sets up the
            distributed execution workflow.
        get_policy_class (Optional[Callable[
            TrainerConfigDict, Optional[Type[Policy]]]]): Optional callable
            that takes a config and returns the policy class or None. If None
            is returned, will use `default_policy` (which must be provided
            then).
        validate_config (Optional[Callable[[TrainerConfigDict], None]]):
            Optional callable that takes the config to check for correctness.
            It may mutate the config as needed.
        validate_env (Optional[Callable[[EnvType, EnvContext], None]]):
            Optional callable to validate the generated environment (only
            on worker=0).
        before_init (Optional[Callable[[Trainer], None]]): Now: event
            "before_make_workers".
            Optional callable to run before anything is constructed inside
            Trainer (Workers with Policies, execution plan, etc..). Takes the
            Trainer instance as argument.
        after_init (Optional[Callable[[Trainer], None]]): Now event
            "after_make_workers".
            Optional callable to run at the end of trainer init (after all
            Workers and the exec. plan have been constructed). Takes the
            Trainer instance as argument.
        before_evaluate_fn (Optional[Callable[[Trainer], None]]): Now event
            "before_evaluate".
            Callback to run before evaluation. This takes the trainer instance
            as argument.

    Returns:
        Type[Trainer]: A Trainer sub-class configured by the specified args.
    """
    original_kwargs = locals().copy()

    # Set up event_subscriptions dict.
    event_subscriptions = defaultdict(list, event_subscriptions or {})
    for event, handler in event_subscriptions.copy().items():
        if not isinstance(handler, list):
            event_subscriptions[event] = force_list(handler)

    # Handle deprecated kwargs.
    if validate_config != DEPRECATED_VALUE:
        # deprecation_warning(
        #    old="validate_config",
        #    new=f"event_subscriptions[{events.AFTER_CONFIG_COMPLETE}]",
        #    error=False)
        # Shim new `trainer` arg (backward compatibility).
        event_subscriptions[events.AFTER_CONFIG_COMPLETE].append(
            lambda trainer, config: validate_config(config))
    if validate_env != DEPRECATED_VALUE:
        # deprecation_warning(
        #    old="`validate_env`",
        #    new=f"event_subscriptions[{events.AFTER_VALIDATE_ENV}]",
        #    error=False)
        # Shim new `trainer` arg (backward compatibility).
        event_subscriptions[events.AFTER_VALIDATE_ENV].append(
            lambda worker, env, env_ctx: validate_env(env, env_ctx))
    if before_evaluate_fn != DEPRECATED_VALUE:
        # deprecation_warning(
        #    old="before_evaluate_fn",
        #    new="event_subscriptions['before_evaluate']", error=False)
        event_subscriptions[events.BEFORE_EVALUATE].append(before_evaluate_fn)
    if before_init != DEPRECATED_VALUE:
        # deprecation_warning(
        #    "before_init",
        #    f"event_subscriptions[{events.BEFORE_CREATE_ROLLOUT_WORKERS}]",
        #    error=False)
        event_subscriptions[events.BEFORE_CREATE_ROLLOUT_WORKERS].append(
            before_init)
    if after_init != DEPRECATED_VALUE:
        # deprecation_warning(
        #    old="after_init",
        #    new=f"event_subscriptions[{events.AFTER_CREATE_ROLLOUT_WORKERS}]",
        #    error=False)
        event_subscriptions[events.AFTER_CREATE_ROLLOUT_WORKERS].append(
            after_init)
    if get_policy_class != DEPRECATED_VALUE:
        # deprecation_warning(
        #    old="get_policy_class",
        #    new="event_subscriptions[{events.SUGGEST_DEFAULT_POLICY_CLASS}]",
        #    error=False)
        # Shim new `trainer` arg (backward compatibility).
        event_subscriptions[events.SUGGEST_DEFAULT_POLICY_CLASS].append(
            lambda trainer, config: get_policy_class(config))
    if execution_plan != DEPRECATED_VALUE:
        # deprecation_warning(
        #    old="`execution_plan`",
        #    new=f"event_subscriptions[{events.SUGGEST_EXECUTION_PLAN}]",
        #    error=False)
        event_subscriptions[events.SUGGEST_EXECUTION_PLAN].append(
            execution_plan)
    # Make sure we use the default_execution plan if nothing else.
    if events.SUGGEST_EXECUTION_PLAN not in event_subscriptions:
        event_subscriptions[events.SUGGEST_EXECUTION_PLAN] = \
            [default_execution_plan]

    base = add_mixins(Trainer, mixins)

    class trainer_cls(base):
        _name = name
        _default_config = default_config or COMMON_CONFIG
        _policy_class = default_policy

        def __init__(self, config=None, env=None, logger_creator=None):
            config = config or {}

            # Bake provided subscriptions into this Trainer sub-class' c'tor
            # w/o altering the passed in `config`.
            config = config.copy()
            conf_ = config.get("_event_subscriptions") or defaultdict(list)
            for event_name, handlers in event_subscriptions.items():
                conf_[event_name].extend(handlers)
            config["_event_subscriptions"] = conf_

            if allow_unknown_subkeys is not None:
                self._allow_unknown_subkeys += allow_unknown_subkeys
            self._allow_unknown_configs = allow_unknown_configs
            if override_all_subkeys_if_type_changes is not None:
                self._override_all_subkeys_if_type_changes += \
                    override_all_subkeys_if_type_changes

            Trainer.__init__(self, config, env, logger_creator)

        def _init(self, config: TrainerConfigDict,
                  env_creator: Callable[[EnvConfigDict], EnvType]):

            # Call subscribers to suggest a default policy class
            # (only one allowed).
            policy_class_suggestion = self.trigger_event(events.SUGGEST_DEFAULT_POLICY_CLASS, config)
            # Only override self._policy_class if we have a suggestion.
            if policy_class_suggestion is not None:
                self._policy_class = policy_class_suggestion

            # Creating all workers (excluding evaluation workers).
            self.workers = self.make_workers(
                env_creator=env_creator,
                policy_class=self._policy_class,
                config=config,
                num_workers=self.config["num_workers"])

            # Call subscribers to suggest an execution plan (only one allowed or
            # None).
            self.execution_plan = self.trigger_event(
                events.SUGGEST_EXECUTION_PLAN, self.workers, config)

        @staticmethod
        @override(Trainer)
        def with_updates(**overrides) -> Type[Trainer]:
            """Build a copy of this trainer class with the specified overrides.

            Keyword Args:
                overrides (dict): use this to override any of the arguments
                    originally passed to build_trainer_class() for this policy.

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
            return build_trainer_class(**dict(original_kwargs, **overrides))

    trainer_cls.__name__ = name
    trainer_cls.__qualname__ = name
    return trainer_cls


@Deprecated(new="build_trainer_class", error=False)
def build_trainer(*args, **kwargs):
    return build_trainer_class(*args, **kwargs)
