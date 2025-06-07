import logging
from dataclasses import dataclass

from ray.train._internal.syncer import SyncConfig as TrainSyncConfig
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
@dataclass
class SyncConfig(TrainSyncConfig):
    """Configuration object for Tune file syncing to `RunConfig(storage_path)`.

    In Ray Tune, here is where syncing (mainly uploading) happens:

    The experiment driver (on the head node) syncs the experiment directory to storage
    (which includes experiment state such as searcher state, the list of trials
    and their statuses, and trial metadata).

    It's also possible to sync artifacts from the trial directory to storage
    by setting `sync_artifacts=True`.
    For a Ray Tune run with many trials, each trial will upload its trial directory
    to storage, which includes arbitrary files that you dumped during the run.

    Args:
        sync_period: Minimum time in seconds to wait between two sync operations.
            A smaller ``sync_period`` will have the data in storage updated more often
            but introduces more syncing overhead. Defaults to 5 minutes.
        sync_timeout: Maximum time in seconds to wait for a sync process
            to finish running. A sync operation will run for at most this long
            before raising a `TimeoutError`. Defaults to 30 minutes.
        sync_artifacts: [Beta] Whether or not to sync artifacts that are saved to the
            trial directory (accessed via `ray.tune.get_context().get_trial_dir()`)
            to the persistent storage configured via `tune.RunConfig(storage_path)`.
            The trial or remote worker will try to launch an artifact syncing
            operation every time `tune.report` happens, subject to `sync_period`
            and `sync_artifacts_on_checkpoint`.
            Defaults to False -- no artifacts are persisted by default.
        sync_artifacts_on_checkpoint: If True, trial/worker artifacts are
            forcefully synced on every reported checkpoint.
            This only has an effect if `sync_artifacts` is True.
            Defaults to True.
    """

    pass
