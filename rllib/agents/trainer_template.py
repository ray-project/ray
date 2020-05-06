import logging
import os
import time

from ray.rllib.agents.trainer import Trainer, COMMON_CONFIG
from ray.rllib.execution.rollout_ops import ParallelRollouts, ConcatBatches
from ray.rllib.execution.train_ops import TrainOneStep
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.optimizers import SyncSamplesOptimizer
from ray.rllib.utils import add_mixins
from ray.rllib.utils.annotations import override, DeveloperAPI

logger = logging.getLogger(__name__)


def default_execution_plan(workers, config):
    # Collects experiences in parallel from multiple RolloutWorker actors.
    rollouts = ParallelRollouts(workers, mode="bulk_sync")

    # Combine experiences batches until we hit `train_batch_size` in size.
    # Then, train the policy on those experiences and update the workers.
    train_op = rollouts \
        .combine(ConcatBatches(
            min_batch_size=config["train_batch_size"])) \
        .for_each(TrainOneStep(workers))

    # Add on the standard episode reward, etc. metrics reporting. This returns
    # a LocalIterator[metrics_dict] representing metrics for each train step.
    return StandardMetricsReporting(train_op, workers, config)


@DeveloperAPI
def build_trainer(name,
                  default_policy,
                  default_config=None,
                  validate_config=None,
                  get_policy_class=None,
                  before_init=None,
                  after_init=None,
                  before_evaluate_fn=None,
                  mixins=None,
                  execution_plan=default_execution_plan):
    """Helper function for defining a custom trainer.

    Functions will be run in this order to initialize the trainer:
        1. Config setup: validate_config, get_policy
        2. Worker setup: before_init, execution_plan
        3. Post setup: after_init

    Arguments:
        name (str): name of the trainer (e.g., "PPO")
        default_policy (cls): the default Policy class to use
        default_config (dict): The default config dict of the algorithm,
            otherwise uses the Trainer default config.
        validate_config (func): optional callback that checks a given config
            for correctness. It may mutate the config as needed.
        get_policy_class (func): optional callback that takes a config and
            returns the policy class to override the default with
        before_init (func): optional function to run at the start of trainer
            init that takes the trainer instance as argument
        make_workers (func): override the method that creates rollout workers.
            This takes in (trainer, env_creator, policy, config) as args.
        after_init (func): optional function to run at the end of trainer init
            that takes the trainer instance as argument
        before_evaluate_fn (func): callback to run before evaluation. This
            takes the trainer instance as argument.
        mixins (list): list of any class mixins for the returned trainer class.
            These mixins will be applied in order and will have higher
            precedence than the Trainer class
        execution_plan (func): Setup the distributed execution workflow.

    Returns:
        a Trainer instance that uses the specified args.
    """

    original_kwargs = locals().copy()
    base = add_mixins(Trainer, mixins)

    class trainer_cls(base):
        _name = name
        _default_config = default_config or COMMON_CONFIG
        _policy = default_policy

        def __init__(self, config=None, env=None, logger_creator=None):
            Trainer.__init__(self, config, env, logger_creator)

        def _init(self, config, env_creator):
            if validate_config:
                validate_config(config)

            if get_policy_class is None:
                self._policy = default_policy
            else:
                self._policy = get_policy_class(config)
            if before_init:
                before_init(self)

            # Creating all workers (excluding evaluation workers).
            self.workers = self._make_workers(env_creator, self._policy,
                                              config,
                                              self.config["num_workers"])
            self.execution_plan = execution_plan
            self.train_exec_impl = execution_plan(self.workers, config)

            if after_init:
                after_init(self)

        @override(Trainer)
        def _train(self):
            return next(self.train_exec_impl)

        @override(Trainer)
        def _before_evaluate(self):
            if before_evaluate_fn:
                before_evaluate_fn(self)

        def __getstate__(self):
            state = Trainer.__getstate__(self)
            if self.train_exec_impl:
                state["train_exec_impl"] = (
                    self.train_exec_impl.shared_metrics.get().save())
            return state

        def __setstate__(self, state):
            Trainer.__setstate__(self, state)
            if self.train_exec_impl:
                self.train_exec_impl.shared_metrics.get().restore(
                    state["train_exec_impl"])

    def with_updates(**overrides):
        """Build a copy of this trainer with the specified overrides.

        Arguments:
            overrides (dict): use this to override any of the arguments
                originally passed to build_trainer() for this policy.
        """
        return build_trainer(**dict(original_kwargs, **overrides))

    trainer_cls.with_updates = staticmethod(with_updates)
    trainer_cls.__name__ = name
    trainer_cls.__qualname__ = name
    return trainer_cls
