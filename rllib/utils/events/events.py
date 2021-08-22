"""
TODO
"""
from typing import Optional

from ray.rllib.utils.events.observable import Observable
from ray.rllib.utils.typing import EventName

# Trainer triggered events.
# -------------------------


#TODO: Sort these in more or less chronological order. From Trainer creation to
#Worker creation to policy creation to rollout/sampling/learning events.

# When the Trainer has to determine a default policy class to use.
# Subscribers should return None (no suggestion) or a Policy sub-class.
# In the single-agent case, this policy class is then used on all
# RolloutWorkers. Same is true for the multi-agent case, but only if no
# class is defined within the PolicySpecs.
SUGGEST_DEFAULT_POLICY_CLASS = "suggest_default_policy_class"


# Triggered once the Trainer's config dict is complete (the user provided
# partial dict has been merged with the Trainer's default config).
AFTER_CONFIG_COMPLETE = "after_config_complete"
# Before the Trainer's `validate_config()` method is called.
BEFORE_VALIDATE_CONFIG = "before_validate_config"
# After the Trainer's `validate_config()` method is called.
AFTER_VALIDATE_CONFIG = "after_validate_config"

# Before the Trainer's "base" RolloutWorker set is created. This set will be
# stored under property: `trainer.workers`.
BEFORE_CREATE_ROLLOUT_WORKERS = "before_create_rollout_workers"
# After the Trainer's "base" RolloutWorker set has been created.
AFTER_CREATE_ROLLOUT_WORKERS = "after_create_rollout_workers"

# Before the Trainer's evaluation worker set is created. This set will be
# stored under property: `trainer.evaluation_workers`. Note that
# this event will not be triggerd if no evaluation workers are setup
# (config.evaluation_interval=None).
BEFORE_CREATE_EVALUATION_WORKERS = "before_create_evaluation_workers"
# After the Trainer's evaluation worker set has been created. Note that
# # this event will not be triggerd if no evaluation workers are setup
# # (config.evaluation_interval=None).
AFTER_CREATE_EVALUATION_WORKERS = "after_create_evaluation_workers"

# When the Trainer's execution plan (if applicable) should be created.
# Subscribers need to set the `trainer.execution_plan` property to
# a ray.util.iter.LocalIterator object that will then be iterated over
# on each train iteration (`trainer.train()` call).
SUGGEST_EXECUTION_PLAN = "suggest_execution_plan"

# Before the execution plan of the Trainer is called (which creates a
# LocalIterator used for train.train() iterations).
BEFORE_CREATE_EXECUTION_PLAN = "before_create_execution_plan"
# After the execution plan of the Trainer is called.
AFTER_CREATE_EXECUTION_PLAN = "after_create_execution_plan"

# Before the Trainer's `evaluate()` method is called.
# This replaces the deprecated: `before_evaluate_fn` of the
# `build_trainer_class()` util function.
BEFORE_EVALUATE = "before_evaluate"
# After the Trainer's `evaluate()` method is called.
AFTER_EVALUATE = "after_evaluate"

# Worker triggered events.
# ------------------------
# Before the "base" validation function for an env has been called.
BEFORE_VALIDATE_ENV = "before_validate_env"
# After the "base" validation function for an env has been called. This
# would be a good moment for subscribers to perform custom env checks. 
AFTER_VALIDATE_ENV = "after_validate_env"


# Policy triggered events.
# ------------------------
#TODO


def TriggersEvent(*,
                  name: Optional[EventName] = None,
                  before: bool = True,
                  after: bool = True):

    def _inner(obj):

        def patched(*args, **kwargs):
            # Try to extract self.
            self: Observable = obj.__self__ if hasattr(obj, "__self__") else \
                args[0]
            if not isinstance(self, Observable):
                raise ValueError("`@TriggersEvent` not a valid decorator for "
                                 "non-Observable methods!")

            # Shift out self from args used by triggers as it'll be prepended
            # by `Observable.trigger_event()` anyways.
            trigger_args = args
            if len(args) and self is args[0]:
                trigger_args = args[1:]

            event_base = name or obj.__name__
            # Trigger `before` event passing all args and kwargs as-is to the
            # subscribed event handler(s).
            if before:
                self.trigger_event(f"before_{event_base}", *trigger_args, **kwargs)

            ret = obj(*args, **kwargs)

            # Trigger `after` event passing all args and kwargs as-is plus
            # the return values to the subscribed event handler(s).
            if after:
                try:
                    self.trigger_event(
                        f"after_{event_base}",
                        *trigger_args, **kwargs,
                        return_values=ret)
                # Give user the chance to ignore the auto-appended `return_value`
                # keyword arg.
                except TypeError as e:
                    if "unexpected keyword argument 'return_values'" in e.args[0]:
                        ret = self.trigger_event(f"after_{event_base}", *trigger_args, **kwargs)
                    else:
                        raise e

            # Leave return vlues as-is.
            return ret

        return patched

    return _inner
