import os
from typing import List, Optional

from ray.tune.callback import Callback
from ray.tune.progress_reporter import TrialProgressCallback
from ray.tune.syncer import SyncConfig
from ray.tune.syncer import SyncerCallback


def create_default_callbacks(callbacks: Optional[List[Callback]],
                             sync_config: SyncConfig,
                             metric: Optional[str] = None):
    callbacks = callbacks or []

    # Check if we have a Syncer and a TrialProgress callback
    has_syncer_callback = any(isinstance(c, SyncerCallback) for c in callbacks)
    has_trial_progress_callback = any(
        isinstance(c, TrialProgressCallback) for c in callbacks)

    if not has_trial_progress_callback:
        trial_progress_callback = TrialProgressCallback(metric=metric)
        callbacks.append(trial_progress_callback)

    # If no SyncerCallback was found, add
    if not has_syncer_callback and os.environ.get(
            "TUNE_DISABLE_AUTO_CALLBACK_SYNCER", "0") != "1":
        syncer_callback = SyncerCallback(
            sync_function=sync_config.sync_to_driver)
        callbacks.append(syncer_callback)

    return callbacks
