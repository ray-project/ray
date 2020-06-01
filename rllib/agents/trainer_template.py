import logging
import time

from ray.rllib.agents.trainer import Trainer, COMMON_CONFIG
from ray.rllib.execution.rollout_ops import ParallelRollouts, ConcatBatches
from ray.rllib.execution.train_ops import TrainOneStep
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.utils import add_mixins
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.deprecation import deprecation_warning

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
def build_trainer(
        name,
        default_policy,
        default_config=None,
        validate_config=None,
        get_initial_state=None,  # DEPRECATED
        get_policy_class=None,
        before_init=None,
        make_workers=None,  # DEPRECATED
        make_policy_optimizer=None,  # DEPRECATED
        after_init=None,
        before_train_step=None,  # DEPRECATED
        after_optimizer_step=None,  # DEPRECATED
        after_train_result=None,  # DEPRECATED
        collect_metrics_fn=None,  # DEPRECATED
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

            if get_initial_state:
                deprecation_warning("get_initial_state", "execution_plan")
                self.state = get_initial_state(self)
            else:
                self.state = {}
            if get_policy_class is None:
                self._policy = default_policy
            else:
                self._policy = get_policy_class(config)
            if before_init:
                before_init(self)
            # Creating all workers (excluding evaluation workers).
            if make_workers and not execution_plan:
                deprecation_warning("make_workers", "execution_plan")
                self.workers = make_workers(self, env_creator, self._policy,
                                            config)
            else:
                self.workers = self._make_workers(env_creator, self._policy,
                                                  config,
                                                  self.config["num_workers"])
            self.train_exec_impl = None
            self.optimizer = None
            self.execution_plan = execution_plan

            if make_policy_optimizer:
                deprecation_warning("make_policy_optimizer", "execution_plan")
                self.optimizer = make_policy_optimizer(self.workers, config)
            else:
                assert execution_plan is not None
                self.train_exec_impl = execution_plan(self.workers, config)
            if after_init:
                after_init(self)

        @override(Trainer)
        def _train(self):
            if self.train_exec_impl:
                return self._train_exec_impl()

            if before_train_step:
                deprecation_warning("before_train_step", "execution_plan")
                before_train_step(self)
            prev_steps = self.optimizer.num_steps_sampled

            start = time.time()
            optimizer_steps_this_iter = 0
            while True:
                fetches = self.optimizer.step()
                optimizer_steps_this_iter += 1
                if after_optimizer_step:
                    deprecation_warning("after_optimizer_step",
                                        "execution_plan")
                    after_optimizer_step(self, fetches)
                if (time.time() - start >= self.config["min_iter_time_s"]
                        and self.optimizer.num_steps_sampled - prev_steps >=
                        self.config["timesteps_per_iteration"]):
                    break

            if collect_metrics_fn:
                deprecation_warning("collect_metrics_fn", "execution_plan")
                res = collect_metrics_fn(self)
            else:
                res = self.collect_metrics()
            res.update(
                optimizer_steps_this_iter=optimizer_steps_this_iter,
                timesteps_this_iter=self.optimizer.num_steps_sampled -
                prev_steps,
                info=res.get("info", {}))

            if after_train_result:
                deprecation_warning("after_train_result", "execution_plan")
                after_train_result(self, res)
            return res

        def _train_exec_impl(self):
            res = next(self.train_exec_impl)
            return res

        @override(Trainer)
        def _before_evaluate(self):
            if before_evaluate_fn:
                before_evaluate_fn(self)

        def __getstate__(self):
            state = Trainer.__getstate__(self)
            state["trainer_state"] = self.state.copy()
            if self.train_exec_impl:
                state["train_exec_impl"] = (
                    self.train_exec_impl.shared_metrics.get().save())
            return state

        def __setstate__(self, state):
            Trainer.__setstate__(self, state)
            self.state = state["trainer_state"].copy()
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
