import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Set

import pyarrow
import yaml

from ray.air._internal.json import SafeFallbackEncoder
from ray.tune.callback import Callback
from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    from ray.tune.experiment.trial import Trial  # noqa: F401

logger = logging.getLogger(__name__)


# Apply flow style for sequences of this length
_SEQUENCE_LEN_FLOW_STYLE = 3


@PublicAPI
class LoggerCallback(Callback):
    """Base class for experiment-level logger callbacks

    This base class defines a general interface for logging events,
    like trial starts, restores, ends, checkpoint saves, and receiving
    trial results.

    Callbacks implementing this interface should make sure that logging
    utilities are cleaned up properly on trial termination, i.e. when
    ``log_trial_end`` is received. This includes e.g. closing files.
    """

    def log_trial_start(self, trial: "Trial"):
        """Handle logging when a trial starts.

        Args:
            trial: Trial object.
        """
        pass

    def log_trial_restore(self, trial: "Trial"):
        """Handle logging when a trial restores.

        Args:
            trial: Trial object.
        """
        pass

    def log_trial_save(self, trial: "Trial"):
        """Handle logging when a trial saves a checkpoint.

        Args:
            trial: Trial object.
        """
        pass

    def log_trial_result(self, iteration: int, trial: "Trial", result: Dict):
        """Handle logging when a trial reports a result.

        Args:
            trial: Trial object.
            result: Result dictionary.
        """
        pass

    def log_trial_end(self, trial: "Trial", failed: bool = False):
        """Handle logging when a trial ends.

        Args:
            trial: Trial object.
            failed: True if the Trial finished gracefully, False if
                it failed (e.g. when it raised an exception).
        """
        pass

    def on_trial_result(
        self,
        iteration: int,
        trials: List["Trial"],
        trial: "Trial",
        result: Dict,
        **info,
    ):
        self.log_trial_result(iteration, trial, result)

    def on_trial_start(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        self.log_trial_start(trial)

    def on_trial_restore(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        self.log_trial_restore(trial)

    def on_trial_save(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        self.log_trial_save(trial)

    def on_trial_complete(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        self.log_trial_end(trial, failed=False)

    def on_trial_error(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        self.log_trial_end(trial, failed=True)

    def _restore_from_remote(self, file_name: str, trial: "Trial") -> None:
        if not trial.checkpoint:
            # If there's no checkpoint, there's no logging artifacts to restore
            # since we're starting from scratch.
            return

        local_file = Path(trial.local_path, file_name).as_posix()
        remote_file = Path(trial.storage.trial_fs_path, file_name).as_posix()

        try:
            pyarrow.fs.copy_files(
                remote_file,
                local_file,
                source_filesystem=trial.storage.storage_filesystem,
            )
            logger.debug(f"Copied {remote_file} to {local_file}")
        except FileNotFoundError:
            logger.warning(f"Remote file not found: {remote_file}")
        except Exception:
            logger.exception(f"Error downloading {remote_file}")


class _RayDumper(yaml.SafeDumper):
    def represent_sequence(self, tag, sequence, flow_style=None):
        if len(sequence) > _SEQUENCE_LEN_FLOW_STYLE:
            return super().represent_sequence(tag, sequence, flow_style=True)
        return super().represent_sequence(tag, sequence, flow_style=flow_style)


@DeveloperAPI
def pretty_print(result, exclude: Optional[Set[str]] = None):
    result = result.copy()
    result.update(config=None)  # drop config from pretty print
    result.update(hist_stats=None)  # drop hist_stats from pretty print
    out = {}
    for k, v in result.items():
        if v is not None and (exclude is None or k not in exclude):
            out[k] = v

    cleaned = json.dumps(out, cls=SafeFallbackEncoder)
    return yaml.dump(json.loads(cleaned), Dumper=_RayDumper, default_flow_style=False)
