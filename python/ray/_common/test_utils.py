# Backward compatibility imports
# These classes have been moved to ray._common.tests.test_utils
# This file maintains backward compatibility for existing imports

from ray._common.tests.test_utils import SignalActor, Semaphore

__all__ = ["SignalActor", "Semaphore"]
