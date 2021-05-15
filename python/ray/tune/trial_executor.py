# coding: utf-8
import logging

from ray.tune.trial import Trial, Checkpoint
from ray.tune.error import TuneError
from ray.tune.cluster_info import is_ray_cluster

logger = logging.getLogger(__name__)


class TrialExecutor:
    """Module for interacting with remote trainables.

    Manages platform-specific details such as resource handling
    and starting/stopping trials.
    """

    def __init__(self, queue_trials: bool = False):
        """Initializes a new TrialExecutor.

        Args:
            queue_trials (bool): Whether to queue trials when the cluster does
                not currently have enough resources to launch one. This should
                be set to True when running on an autoscaling cluster to enable
                automatic scale-up.
        """
        self._queue_trials = queue_trials
        self._cached_trial_state = {}
        self._trials_to_cache = set()

    def set_status(self, trial, status):
        """Sets status and checkpoints metadata if needed.

        Only checkpoints metadata if trial status is a terminal condition.
        PENDING, PAUSED, and RUNNING switches have checkpoints taken care of
        in the TrialRunner.

        Args:
            trial (Trial): Trial to checkpoint.
            status (Trial.status): Status to set trial to.
        """
        if trial.status == status:
            logger.debug("Trial %s: Status %s unchanged.", trial, trial.status)
        else:
            logger.debug("Trial %s: Changing status from %s to %s.", trial,
                         trial.status, status)
        trial.set_status(status)
        if status in [Trial.TERMINATED, Trial.ERROR]:
            self.try_checkpoint_metadata(trial)

    def try_checkpoint_metadata(self, trial):
        """Checkpoints trial metadata.

        Args:
            trial (Trial): Trial to checkpoint.
        """
        if trial.checkpoint.storage == Checkpoint.MEMORY:
            logger.debug("Trial %s: Not saving data for memory checkpoint.",
                         trial)
            return
        try:
            logger.debug("Trial %s: Saving trial metadata.", trial)
            # Lazy cache trials
            self._trials_to_cache.add(trial)
        except Exception:
            logger.exception("Trial %s: Error checkpointing trial metadata.",
                             trial)

    def get_checkpoints(self):
        """Returns a copy of mapping of the trial ID to pickled metadata."""
        for trial in self._trials_to_cache:
            self._cached_trial_state[trial.trial_id] = trial.get_json_state()
        self._trials_to_cache.clear()
        return self._cached_trial_state

    def has_resources(self, resources):
        """Returns whether this runner has at least the specified resources."""
        raise NotImplementedError("Subclasses of TrialExecutor must provide "
                                  "has_resources() method")

    def start_trial(self, trial, checkpoint=None, train=True) -> bool:
        """Starts the trial restoring from checkpoint if checkpoint is provided.

        Args:
            trial (Trial): Trial to be started.
            checkpoint (Checkpoint): A Python object or path storing the state
            of trial.
            train (bool): Whether or not to start training.

        Returns:
            True if trial started successfully, False otherwise.
        """
        raise NotImplementedError("Subclasses of TrialExecutor must provide "
                                  "start_trial() method")

    def stop_trial(self, trial, error=False, error_msg=None):
        """Stops the trial.

        Stops this trial, releasing all allocating resources.
        If stopping the trial fails, the run will be marked as terminated
        in error, but no exception will be thrown.

        Args:
            error (bool): Whether to mark this trial as terminated in error.
            error_msg (str): Optional error message.

        """
        raise NotImplementedError("Subclasses of TrialExecutor must provide "
                                  "stop_trial() method")

    def continue_training(self, trial):
        """Continues the training of this trial."""
        pass

    def pause_trial(self, trial):
        """Pauses the trial.

        We want to release resources (specifically GPUs) when pausing an
        experiment. This results in PAUSED state that similar to TERMINATED.
        """
        assert trial.status == Trial.RUNNING, trial.status
        try:
            self.save(trial, Checkpoint.MEMORY)
            self.stop_trial(trial)
            self.set_status(trial, Trial.PAUSED)
        except Exception:
            logger.exception("Error pausing runner.")
            self.set_status(trial, Trial.ERROR)

    def unpause_trial(self, trial):
        """Sets PAUSED trial to pending to allow scheduler to start."""
        assert trial.status == Trial.PAUSED, trial.status
        self.set_status(trial, Trial.PENDING)

    def resume_trial(self, trial):
        """Resumes PAUSED trials. This is a blocking call."""
        assert trial.status == Trial.PAUSED, trial.status
        self.start_trial(trial)

    def reset_trial(self, trial, new_config, new_experiment_tag):
        """Tries to invoke `Trainable.reset()` to reset trial.

        Args:
            trial (Trial): Trial to be reset.
            new_config (dict): New configuration for Trial
                trainable.
            new_experiment_tag (str): New experiment name
                for trial.

        Returns:
            True if `reset` is successful else False.
        """
        raise NotImplementedError

    def get_running_trials(self):
        """Returns all running trials."""
        raise NotImplementedError("Subclasses of TrialExecutor must provide "
                                  "get_running_trials() method")

    def on_step_begin(self, trial_runner):
        """A hook called before running one step of the trial event loop."""
        pass

    def on_step_end(self, trial_runner):
        """A hook called after running one step of the trial event loop."""
        pass

    def on_no_available_trials(self, trial_runner):
        if self._queue_trials:
            return
        for trial in trial_runner.get_trials():
            if trial.uses_placement_groups:
                return
            if trial.status == Trial.PENDING:
                if not self.has_resources_for_trial(trial):
                    resource_string = trial.resources.summary_string()
                    trial_resource_help_msg = trial.get_trainable_cls(
                    ).resource_help(trial.config)
                    autoscaling_msg = ""
                    if is_ray_cluster():
                        autoscaling_msg = (
                            "Pass `queue_trials=True` in ray.tune.run() or "
                            "on the command line to queue trials until the "
                            "cluster scales up or resources become available. "
                        )
                    raise TuneError(
                        "Insufficient cluster resources to launch trial: "
                        f"trial requested {resource_string}, but the cluster "
                        f"has only {self.resource_string()}. "
                        f"{autoscaling_msg}"
                        f"{trial_resource_help_msg} ")
            elif trial.status == Trial.PAUSED:
                raise TuneError("There are paused trials, but no more pending "
                                "trials with sufficient resources.")

    def get_next_available_trial(self):
        """Blocking call that waits until one result is ready.

        Returns:
            Trial object that is ready for intermediate processing.
        """
        raise NotImplementedError

    def get_next_failed_trial(self):
        """Non-blocking call that detects and returns one failed trial.

        Returns:
            A Trial object that is ready for failure processing. None if
            no failure detected.
        """
        raise NotImplementedError

    def fetch_result(self, trial):
        """Fetches one result for the trial.

        Assumes the trial is running.

        Returns:
            Result object for the trial.
        """
        raise NotImplementedError

    def debug_string(self):
        """Returns a human readable message for printing to the console."""
        raise NotImplementedError

    def resource_string(self):
        """Returns a string describing the total resources available."""
        raise NotImplementedError

    def restore(self, trial, checkpoint=None, block=False):
        """Restores training state from a checkpoint.

        If checkpoint is None, try to restore from trial.checkpoint.
        If restoring fails, the trial status will be set to ERROR.

        Args:
            trial (Trial): Trial to be restored.
            checkpoint (Checkpoint): Checkpoint to restore from.
            block (bool): Whether or not to block on restore before returning.

        Returns:
            False if error occurred, otherwise return True.
        """
        raise NotImplementedError("Subclasses of TrialExecutor must provide "
                                  "restore() method")

    def save(self, trial, storage=Checkpoint.PERSISTENT, result=None):
        """Saves training state of this trial to a checkpoint.

        If result is None, this trial's last result will be used.

        Args:
            trial (Trial): The state of this trial to be saved.
            storage (str): Where to store the checkpoint. Defaults to
                PERSISTENT.
            result (dict): The state of this trial as a dictionary to be saved.

        Returns:
            A Checkpoint object.
        """
        raise NotImplementedError("Subclasses of TrialExecutor must provide "
                                  "save() method")

    def export_trial_if_needed(self, trial):
        """Exports model of this trial based on trial.export_formats.

        Args:
            trial (Trial): The state of this trial to be saved.

        Returns:
            A dict that maps ExportFormats to successfully exported models.
        """
        raise NotImplementedError("Subclasses of TrialExecutor must provide "
                                  "export_trial_if_needed() method")

    def has_gpus(self):
        """Returns True if GPUs are detected on the cluster."""
        return None

    def cleanup(self, trial_runner):
        """Ensures that trials are cleaned up after stopping."""
        pass

    def in_staging_grace_period(self) -> bool:
        """Returns True if trials have recently been staged."""
        return False

    def set_max_pending_trials(self, max_pending: int):
        """Set the maximum number of allowed pending trials."""
        pass
