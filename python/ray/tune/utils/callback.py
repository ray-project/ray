import os
from typing import List, Optional

from ray.tune.callback import Callback
from ray.tune.syncer import SyncConfig
from ray.tune.syncer import SyncerCallback


def create_default_callbacks(callbacks: Optional[List[Callback]],
                             sync_config: SyncConfig):

    callbacks = callbacks or []
    has_syncer_callback = False

    # Check if we have a CSV and JSON logger
    for i, callback in enumerate(callbacks):
        if isinstance(callback, SyncerCallback):
            has_syncer_callback = True

    # If no SyncerCallback was found, add
    if not has_syncer_callback and os.environ.get(
            "TUNE_DISABLE_AUTO_CALLBACK_SYNCER", "0") != "1":
        syncer_callback = SyncerCallback(
            sync_function=sync_config.sync_to_driver)
        callbacks.append(syncer_callback)

    return callbacks
