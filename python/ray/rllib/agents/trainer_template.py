from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.trainer import Trainer, COMMON_CONFIG
from ray.rllib.optimizers import SyncSamplesOptimizer
from ray.rllib.utils.annotations import override, DeveloperAPI


@DeveloperAPI
def build_trainer(name,
                  default_policy,
                  default_config=None,
                  make_policy_optimizer=None,
                  validate_config=None,
                  get_policy_class=None,
                  before_train_step=None,
                  after_optimizer_step=None,
                  after_train_result=None):
    """Helper function for defining a custom trainer.

    Arguments:
        name (str): name of the trainer (e.g., "PPO")
        default_policy (cls): the default Policy class to use
        default_config (dict): the default config dict of the algorithm,
            otherwises uses the Trainer default config
        make_policy_optimizer (func): optional function that returns a
            PolicyOptimizer instance given
            (local_evaluator, remote_evaluators, config)
        validate_config (func): optional callback that checks a given config
            for correctness. It may mutate the config as needed.
        get_policy_class (func): optional callback that takes a config and
            returns the policy class to override the default with
        before_train_step (func): optional callback to run before each train()
            call. It takes the trainer instance as an argument.
        after_optimizer_step (func): optional callback to run after each
            step() call to the policy optimizer. It takes the trainer instance
            and the policy gradient fetches as arguments.
        after_train_result (func): optional callback to run at the end of each
            train() call. It takes the trainer instance and result dict as
            arguments, and may mutate the result dict as needed.

    Returns:
        a Trainer instance that uses the specified args.
    """

    if not name.endswith("Trainer"):
        raise ValueError("Algorithm name should have *Trainer suffix", name)

    class trainer_cls(Trainer):
        _name = name
        _default_config = default_config or COMMON_CONFIG
        _policy = default_policy

        def _init(self, config, env_creator):
            if validate_config:
                validate_config(config)
            if get_policy_class is None:
                policy = default_policy
            else:
                policy = get_policy_class(config)
            self.local_evaluator = self.make_local_evaluator(
                env_creator, policy)
            self.remote_evaluators = self.make_remote_evaluators(
                env_creator, policy, config["num_workers"])
            if make_policy_optimizer:
                self.optimizer = make_policy_optimizer(
                    self.local_evaluator, self.remote_evaluators, config)
            else:
                optimizer_config = dict(
                    config["optimizer"],
                    **{"train_batch_size": config["train_batch_size"]})
                self.optimizer = SyncSamplesOptimizer(self.local_evaluator,
                                                      self.remote_evaluators,
                                                      **optimizer_config)

        @override(Trainer)
        def _train(self):
            if before_train_step:
                before_train_step(self)
            prev_steps = self.optimizer.num_steps_sampled
            fetches = self.optimizer.step()
            if after_optimizer_step:
                after_optimizer_step(self, fetches)
            res = self.collect_metrics()
            res.update(
                timesteps_this_iter=self.optimizer.num_steps_sampled -
                prev_steps,
                info=res.get("info", {}))
            if after_train_result:
                after_train_result(self, res)
            return res

    trainer_cls.__name__ = name
    trainer_cls.__qualname__ = name
    return trainer_cls
