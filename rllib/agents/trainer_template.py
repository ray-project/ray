from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

from ray.rllib.agents.trainer import Trainer, COMMON_CONFIG
from ray.rllib.optimizers import SyncSamplesOptimizer
from ray.rllib.utils import add_mixins
from ray.rllib.utils.annotations import override, DeveloperAPI


@DeveloperAPI
def build_trainer(name,
                  default_policy,
                  default_config=None,
                  validate_config=None,
                  get_initial_state=None,
                  get_policy_class=None,
                  before_init=None,
                  make_workers=None,
                  make_policy_optimizer=None,
                  after_init=None,
                  before_train_step=None,
                  after_optimizer_step=None,
                  after_train_result=None,
                  collect_metrics_fn=None,
                  before_evaluate_fn=None,
                  mixins=None):
    """Helper function for defining a custom trainer.

    Functions will be run in this order to initialize the trainer:
        1. Config setup: validate_config, get_initial_state, get_policy
        2. Worker setup: before_init, make_workers, make_policy_optimizer
        3. Post setup: after_init

    Arguments:
        name (str): name of the trainer (e.g., "PPO")
        default_policy (cls): the default Policy class to use
        default_config (dict): the default config dict of the algorithm,
            otherwises uses the Trainer default config
        validate_config (func): optional callback that checks a given config
            for correctness. It may mutate the config as needed.
        get_initial_state (func): optional function that returns the initial
            state dict given the trainer instance as an argument. The state
            dict must be serializable so that it can be checkpointed, and will
            be available as the `trainer.state` variable.
        get_policy_class (func): optional callback that takes a config and
            returns the policy class to override the default with
        before_init (func): optional function to run at the start of trainer
            init that takes the trainer instance as argument
        make_workers (func): override the method that creates rollout workers.
            This takes in (trainer, env_creator, policy, config) as args.
        make_policy_optimizer (func): optional function that returns a
            PolicyOptimizer instance given (WorkerSet, config)
        after_init (func): optional function to run at the end of trainer init
            that takes the trainer instance as argument
        before_train_step (func): optional callback to run before each train()
            call. It takes the trainer instance as an argument.
        after_optimizer_step (func): optional callback to run after each
            step() call to the policy optimizer. It takes the trainer instance
            and the policy gradient fetches as arguments.
        after_train_result (func): optional callback to run at the end of each
            train() call. It takes the trainer instance and result dict as
            arguments, and may mutate the result dict as needed.
        collect_metrics_fn (func): override the method used to collect metrics.
            It takes the trainer instance as argumnt.
        before_evaluate_fn (func): callback to run before evaluation. This
            takes the trainer instance as argument.
        mixins (list): list of any class mixins for the returned trainer class.
            These mixins will be applied in order and will have higher
            precedence than the Trainer class

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
                self.state = get_initial_state(self)
            else:
                self.state = {}
            if get_policy_class is None:
                policy = default_policy
            else:
                policy = get_policy_class(config)
            if before_init:
                before_init(self)
            if make_workers:
                self.workers = make_workers(self, env_creator, policy, config)
            else:
                self.workers = self._make_workers(env_creator, policy, config,
                                                  self.config["num_workers"])
            if make_policy_optimizer:
                self.optimizer = make_policy_optimizer(self.workers, config)
            else:
                optimizer_config = dict(
                    config["optimizer"],
                    **{"train_batch_size": config["train_batch_size"]})
                self.optimizer = SyncSamplesOptimizer(self.workers,
                                                      **optimizer_config)
            if after_init:
                after_init(self)

        @override(Trainer)
        def _train(self):
            if before_train_step:
                before_train_step(self)
            prev_steps = self.optimizer.num_steps_sampled

            start = time.time()
            while True:
                fetches = self.optimizer.step()
                if after_optimizer_step:
                    after_optimizer_step(self, fetches)
                if (time.time() - start >= self.config["min_iter_time_s"]
                        and self.optimizer.num_steps_sampled - prev_steps >=
                        self.config["timesteps_per_iteration"]):
                    break

            if collect_metrics_fn:
                res = collect_metrics_fn(self)
            else:
                res = self.collect_metrics()
            res.update(
                timesteps_this_iter=self.optimizer.num_steps_sampled -
                prev_steps,
                info=res.get("info", {}))

            if after_train_result:
                after_train_result(self, res)
            return res

        @override(Trainer)
        def _before_evaluate(self):
            if before_evaluate_fn:
                before_evaluate_fn(self)

        def __getstate__(self):
            state = Trainer.__getstate__(self)
            state["trainer_state"] = self.state.copy()
            return state

        def __setstate__(self, state):
            Trainer.__setstate__(self, state)
            self.state = state["trainer_state"].copy()

    @staticmethod
    def with_updates(**overrides):
        """Build a copy of this trainer with the specified overrides.

        Arguments:
            overrides (dict): use this to override any of the arguments
                originally passed to build_trainer() for this policy.
        """
        return build_trainer(**dict(original_kwargs, **overrides))

    trainer_cls.with_updates = with_updates
    trainer_cls.__name__ = name
    trainer_cls.__qualname__ = name
    return trainer_cls
