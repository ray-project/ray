import logging
import datetime
import copy
import os
import aiohttp.web

import ray.dashboard.modules.tune.tune_consts as tune_consts
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray.dashboard.utils import async_loop_forever
from ray.dashboard.optional_utils import rest_response

logger = logging.getLogger(__name__)

try:
    from ray.tune import ExperimentAnalysis
    from tensorboard import program
# The `pip install ray` will not install pandas,
# so `from ray.tune import ExperimentAnalysis` may raises
# `AttributeError: module 'pandas' has no attribute 'core'`
# if the pandas version is incorrect.
except (ImportError, AttributeError) as ex:
    logger.warning("tune module is not available: %s", ex)
    ExperimentAnalysis = None

routes = dashboard_optional_utils.ClassMethodRouteTable


class TuneController(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        """
        This dashboard module is responsible for enabling the Tune tab of
        the dashboard. To do so, it periodically scrapes Tune output logs,
        transforms them, and serves them up over an API.
        """
        super().__init__(dashboard_head)
        self._logdir = None
        self._trial_records = {}
        self._trials_available = False
        self._tensor_board_dir = ""
        self._enable_tensor_board = False
        self._errors = {}

    @routes.get("/tune/info")
    async def tune_info(self, req) -> aiohttp.web.Response:
        stats = self.get_stats()
        return rest_response(success=True, message="Fetched tune info", result=stats)

    @routes.get("/tune/availability")
    async def get_availability(self, req) -> aiohttp.web.Response:
        availability = {
            "available": ExperimentAnalysis is not None,
            "trials_available": self._trials_available,
        }
        return rest_response(
            success=True, message="Fetched tune availability", result=availability
        )

    @routes.get("/tune/set_experiment")
    async def set_tune_experiment(self, req) -> aiohttp.web.Response:
        experiment = req.query["experiment"]
        err, experiment = self.set_experiment(experiment)
        if err:
            return rest_response(success=False, message=err)
        return rest_response(
            success=True, message="Successfully set experiment", **experiment
        )

    @routes.get("/tune/enable_tensorboard")
    async def enable_tensorboard(self, req) -> aiohttp.web.Response:
        self._enable_tensorboard()
        if not self._tensor_board_dir:
            return rest_response(success=False, message="Error enabling tensorboard")
        return rest_response(success=True, message="Enabled tensorboard")

    def get_stats(self):
        tensor_board_info = {
            "tensorboard_current": self._logdir == self._tensor_board_dir,
            "tensorboard_enabled": self._tensor_board_dir != "",
        }
        return {
            "trial_records": copy.deepcopy(self._trial_records),
            "errors": copy.deepcopy(self._errors),
            "tensorboard": tensor_board_info,
        }

    def set_experiment(self, experiment):
        if os.path.isdir(os.path.expanduser(experiment)):
            self._logdir = os.path.expanduser(experiment)
            return None, {"experiment": self._logdir}
        else:
            return "Not a Valid Directory", None

    def _enable_tensorboard(self):
        if not self._tensor_board_dir:
            tb = program.TensorBoard()
            tb.configure(argv=[None, "--logdir", str(self._logdir)])
            tb.launch()
            self._tensor_board_dir = self._logdir

    def collect_errors(self, df):
        sub_dirs = os.listdir(self._logdir)
        trial_names = filter(
            lambda d: os.path.isdir(os.path.join(self._logdir, d)), sub_dirs
        )
        for trial in trial_names:
            error_path = os.path.join(self._logdir, trial, "error.txt")
            if os.path.isfile(error_path):
                self._trials_available = True
                with open(error_path) as f:
                    text = f.read()
                    self._errors[str(trial)] = {
                        "text": text,
                        "job_id": os.path.basename(self._logdir),
                        "trial_id": "No Trial ID",
                    }
                    other_data = df[df["logdir"].str.contains(trial)]
                    if len(other_data) > 0:
                        trial_id = str(other_data["trial_id"].values[0])
                        self._errors[str(trial)]["trial_id"] = trial_id
                        if trial_id in self._trial_records.keys():
                            self._trial_records[trial_id]["error"] = text
                            self._trial_records[trial_id]["status"] = "ERROR"

    @async_loop_forever(tune_consts.TUNE_STATS_UPDATE_INTERVAL_SECONDS)
    async def collect(self):
        """
        Collects and cleans data on the running Tune experiment from the
        Tune logs so that users can see this information in the front-end
        client
        """
        self._trial_records = {}
        self._errors = {}
        if not self._logdir or not ExperimentAnalysis:
            return

        # search through all the sub_directories in log directory
        analysis = ExperimentAnalysis(str(self._logdir))
        df = analysis.dataframe(metric=None, mode=None)

        if len(df) == 0 or "trial_id" not in df.columns:
            return

        self._trials_available = True

        # make sure that data will convert to JSON without error
        df["trial_id_key"] = df["trial_id"].astype(str)
        df = df.fillna(0)

        trial_ids = df["trial_id"]
        for i, value in df["trial_id"].iteritems():
            if type(value) != str and type(value) != int:
                trial_ids[i] = int(value)

        df["trial_id"] = trial_ids

        # convert df to python dict
        df = df.set_index("trial_id_key")
        trial_data = df.to_dict(orient="index")

        # clean data and update class attribute
        if len(trial_data) > 0:
            trial_data = self.clean_trials(trial_data)
            self._trial_records.update(trial_data)

        self.collect_errors(df)

    def clean_trials(self, trial_details):
        first_trial = trial_details[list(trial_details.keys())[0]]
        config_keys = []
        float_keys = []
        metric_keys = []

        # list of static attributes for trial
        default_names = {
            "logdir",
            "time_this_iter_s",
            "done",
            "episodes_total",
            "training_iteration",
            "timestamp",
            "timesteps_total",
            "experiment_id",
            "date",
            "timestamp",
            "time_total_s",
            "pid",
            "hostname",
            "node_ip",
            "time_since_restore",
            "timesteps_since_restore",
            "iterations_since_restore",
            "experiment_tag",
            "trial_id",
        }

        # filter attributes into floats, metrics, and config variables
        for key, value in first_trial.items():
            if isinstance(value, float):
                float_keys.append(key)
            if str(key).startswith("config/"):
                config_keys.append(key)
            elif key not in default_names:
                metric_keys.append(key)

        # clean data into a form that front-end client can handle
        for trial, details in trial_details.items():
            ts = os.path.getctime(details["logdir"])
            formatted_time = datetime.datetime.fromtimestamp(ts).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            details["start_time"] = formatted_time
            details["params"] = {}
            details["metrics"] = {}

            # round all floats
            for key in float_keys:
                details[key] = round(details[key], 12)

            # group together config attributes
            for key in config_keys:
                new_name = key[7:]
                details["params"][new_name] = details[key]
                details.pop(key)

            # group together metric attributes
            for key in metric_keys:
                details["metrics"][key] = details[key]
                details.pop(key)

            if details["done"]:
                details["status"] = "TERMINATED"
            else:
                details["status"] = "RUNNING"
            details.pop("done")

            details["job_id"] = os.path.basename(self._logdir)
            details["error"] = "No Error"

        return trial_details

    async def run(self, server):
        # Forever loop the collection process
        await self.collect()

    @staticmethod
    def is_minimal_module():
        return False
