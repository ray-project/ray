import json
import time

from ray.tune import Callback


class JsonCallback(Callback):
    """Tune Callback to periodically write results to output JSON file."""

    def __init__(self, output_json_file):
        self.output_file = output_json_file

    def on_step_end(self, iteration: int, trials, **info):
        """Write output every 6 tuning loop step."""
        if iteration % 3 == 0:
            current_time = time.time()
            trial_ids = [trial.trial_id for trial in trials]
            trial_names = [str(trial) for trial in trials]
            training_iterations = [
                trial.last_result.get("training_iteration", None)
                for trial in trials
            ]
            trial_statuses = [trial.status for trial in trials]

            output = {
                "last_update": current_time,
                "trial_ids": trial_ids,
                "trial_names": trial_names,
                "training_iterations": training_iterations,
                "trial_statuses": trial_statuses
            }
            with open(self.output_file, "at") as f:
                json.dump(output, f)
