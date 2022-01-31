import logging
from typing import Callable, Iterable, List, Optional, Type, Union

from ray.rllib.agents.trainer import Trainer, COMMON_CONFIG
from ray.rllib.env.env_context import EnvContext
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.policy import Policy
from ray.rllib.utils import add_mixins
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.typing import (
    EnvCreator,
    EnvType,
    PartialTrainerConfigDict,
    ResultDict,
    TrainerConfigDict,
)
from ray.tune.logger import Logger

logger = logging.getLogger(__name__)


@Deprecated(
    new="Sub-class from Trainer (or another Trainer sub-class) directly! "
    "See e.g. ray.rllib.agents.dqn.dqn.py for an example.",
    error=False,
)
def build_trainer(
    name: str,
    *,
    default_config: Optional[TrainerConfigDict] = None,
    validate_config: Optional[Callable[[TrainerConfigDict], None]] = None,
    default_policy: Optional[Type[Policy]] = None,
    get_policy_class: Optional[
        Callable[[TrainerConfigDict], Optional[Type[Policy]]]
    ] = None,
    validate_env: Optional[Callable[[EnvType, EnvContext], None]] = None,
    before_init: Optional[Callable[[Trainer], None]] = None,
    after_init: Optional[Callable[[Trainer], None]] = None,
    before_evaluate_fn: Optional[Callable[[Trainer], None]] = None,
    mixins: Optional[List[type]] = None,
    execution_plan: Optional[
        Union[
            Callable[[WorkerSet, TrainerConfigDict], Iterable[ResultDict]],
            Callable[[Trainer, WorkerSet, TrainerConfigDict], Iterable[ResultDict]],
        ]
    ] = None,
    allow_unknown_configs: bool = False,
    allow_unknown_subkeys: Optional[List[str]] = None,
    override_all_subkeys_if_type_changes: Optional[List[str]] = None,
) -> Type[Trainer]:
    """Helper function for defining a custom Trainer class.

    Functions will be run in this order to initialize the trainer:
        1. Config setup: validate_config, get_policy.
        2. Worker setup: before_init, execution_plan.
        3. Post setup: after_init.

    Args:
        name: name of the trainer (e.g., "PPO")
        default_config: The default config dict of the algorithm,
            otherwise uses the Trainer default config.
        validate_config: Optional callable that takes the config to check
            for correctness. It may mutate the config as needed.
        default_policy: The default Policy class to use if `get_policy_class`
            returns None.
        get_policy_class: Optional callable that takes a config and returns
            the policy class or None. If None is returned, will use
            `default_policy` (which must be provided then).
        validate_env: Optional callable to validate the generated environment
            (only on worker=0).
        before_init: Optional callable to run before anything is constructed
            inside Trainer (Workers with Policies, execution plan, etc..).
            Takes the Trainer instance as argument.
        after_init: Optional callable to run at the end of trainer init
            (after all Workers and the exec. plan have been constructed).
            Takes the Trainer instance as argument.
        before_evaluate_fn: Callback to run before evaluation. This takes
            the trainer instance as argument.
        mixins: List of any class mixins for the returned trainer class.
            These mixins will be applied in order and will have higher
            precedence than the Trainer class.
        execution_plan: Optional callable that sets up the
            distributed execution workflow.
        allow_unknown_configs: Whether to allow unknown top-level config keys.
        allow_unknown_subkeys: List of top-level keys
            with value=dict, for which new sub-keys are allowed to be added to
            the value dict. Appends to Trainer class defaults.
        override_all_subkeys_if_type_changes: List of top level keys with
            value=dict, for which we always override the entire value (dict),
            iff the "type" key in that value dict changes. Appends to Trainer
            class defaults.

    Returns:
        A Trainer sub-class configured by the specified args.
    """

    original_kwargs = locals().copy()
    base = add_mixins(Trainer, mixins)

    class trainer_cls(base):
        _name = name
        _default_config = default_config or COMMON_CONFIG
        _policy_class = default_policy

        def __init__(
            self,
            config: TrainerConfigDict = None,
            env: Union[str, EnvType, None] = None,
            logger_creator: Callable[[], Logger] = None,
            remote_checkpoint_dir: Optional[str] = None,
            sync_function_tpl: Optional[str] = None,
        ):
            Trainer.__init__(
                self,
                config,
                env,
                logger_creator,
                remote_checkpoint_dir,
                sync_function_tpl,
            )

        @override(base)
        def setup(self, config: PartialTrainerConfigDict):
            if allow_unknown_subkeys is not None:
                self._allow_unknown_subkeys += allow_unknown_subkeys
            self._allow_unknown_configs = allow_unknown_configs
            if override_all_subkeys_if_type_changes is not None:
                self._override_all_subkeys_if_type_changes += (
                    override_all_subkeys_if_type_changes
                )
            Trainer.setup(self, config)

        def _init(self, config: TrainerConfigDict, env_creator: EnvCreator):

            # No `get_policy_class` function.
            if get_policy_class is None:
                # Default_policy must be provided (unless in multi-agent mode,
                # where each policy can have its own default policy class).
                if not config["multiagent"]["policies"]:
                    assert default_policy is not None
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
                num_workers=self.config["num_workers"],
            )

            self.train_exec_impl = self.execution_plan(
                self.workers, config, **self._kwargs_for_execution_plan()
            )

            if after_init:
                after_init(self)

        @override(Trainer)
        def validate_config(self, config: PartialTrainerConfigDict):
            # Call super's validation method.
            Trainer.validate_config(self, config)
            # Then call user defined one, if any.
            if validate_config is not None:
                validate_config(config)

        @staticmethod
        @override(Trainer)
        def execution_plan(workers, config, **kwargs):
            # `execution_plan` is provided, use it inside
            # `self.execution_plan()`.
            if execution_plan is not None:
                return execution_plan(workers, config, **kwargs)
            # If `execution_plan` is not provided (None), the Trainer will use
            # it's already existing default `execution_plan()` static method
            # instead.
            else:
                return Trainer.execution_plan(workers, config, **kwargs)

        @override(Trainer)
        def _before_evaluate(self):
            if before_evaluate_fn:
                before_evaluate_fn(self)

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
                >>> from ray.rllib.agents.ppo import PPOTrainer
                >>> MyPPOClass = PPOTrainer.with_updates({"name": "MyPPO"})
                >>> issubclass(MyPPOClass, PPOTrainer)
                False
                >>> issubclass(MyPPOClass, Trainer)
                True
                >>> trainer = MyPPOClass()
                >>> print(trainer)
                MyPPO
            """
            return build_trainer(**dict(original_kwargs, **overrides))

        def __repr__(self):
            return self._name

    trainer_cls.__name__ = name
    trainer_cls.__qualname__ = name
    return trainer_cls
