from typing import Optional

from ray.rllib.utils.typing import EventName


# List of RLlib built-in events.

# Trainer triggered events.
AFTER_VALIDATE_CONFIG = "after_validate_config"

BEFORE_EVALUATE = "before_evaluate"


def TriggersEvent(*,
                  name: Optional[EventName] = None,
                  before: bool = True,
                  after: bool = True):

    def _inner(obj):

        def patched(*args, **kwargs):
            self = 1
            event_name = name or obj.__name__ #TODO
            if before:
                self.trigger_event(f"before_{event_name}")

            ret = obj(*args, **kwargs)

            if after:
                self.trigger_event(f"after_{event_name}", ret)

            return ret

        return patched

    return _inner
