import logging.config
import os
from enum import Enum

import ray
from ray._private.log import PlainRayHandler
from ray._private.ray_logging.filters import CoreContextFilter
from ray._private.ray_logging.formatters import JSONFormatter
from ray.train.v2._internal.execution.context import TrainContext, TrainRunContext
from ray.train.v2._internal.util import get_module_name


def _get_base_logger_config_dict(context: TrainRunContext) -> dict:
    """Return the base logging configuration dictionary."""
    # Using Ray worker ID as the file identifier where logs are written to.
    file_identifier = ray.get_runtime_context().get_worker_id()
    # Return the base logging configuration as a Python dictionary.
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "ray_json": {"class": get_module_name(JSONFormatter)},
        },
        "filters": {
            "core_context_filter": {"()": CoreContextFilter},
            "train_context_filter": {"()": TrainContextFilter, "context": context},
        },
        "handlers": {
            "console": {"class": get_module_name(PlainRayHandler)},
            "file_train_sys_controller": {
                "class": get_module_name(SessionFileHandler),
                "formatter": "ray_json",
                "filename": f"ray-train-sys-controller-{file_identifier}.log",
                "filters": ["core_context_filter", "train_context_filter"],
            },
            "file_train_app_controller": {
                "class": get_module_name(SessionFileHandler),
                "formatter": "ray_json",
                "filename": f"ray-train-app-controller-{file_identifier}.log",
                "filters": ["core_context_filter", "train_context_filter"],
            },
            "file_train_sys_worker": {
                "class": get_module_name(SessionFileHandler),
                "formatter": "ray_json",
                "filename": f"ray-train-sys-worker-{file_identifier}.log",
                "filters": ["core_context_filter", "train_context_filter"],
            },
            "file_train_app_worker": {
                "class": get_module_name(SessionFileHandler),
                "formatter": "ray_json",
                "filename": f"ray-train-app-worker-{file_identifier}.log",
                "filters": ["core_context_filter", "train_context_filter"],
            },
        },
        "loggers": {},
    }


def get_controller_logger_config_dict(context: TrainRunContext) -> dict:
    """Return the controller logger configuration dictionary.

    On the controller process, only the `ray.train` logger is configured.
    This logger emits logs to the following three locations:
        - `file_train_sys_controller`: Ray Train system logs.
        - `file_train_app_controller`: Ray Train application logs.
        - `console`: Logs to the console.
    """

    config_dict = _get_base_logger_config_dict(context)
    config_dict["loggers"]["ray.train"] = {
        "level": "INFO",
        "handlers": [
            "file_train_sys_controller",
            "file_train_app_controller",
            "console",
        ],
        "propagate": False,
    }
    return config_dict


def get_worker_logger_config_dict(context: TrainRunContext) -> dict:
    """Return the worker loggers configuration dictionary.

    On the worker process, there are two loggers being configured:

    First, the `ray.train` logger is configured and emits logs to the
    following three locations:
        - `file_train_sys_worker`: Ray Train system logs.
        - `file_train_app_worker`: Ray Train application logs.
        - `console`: Logs to the console.
    Second, the root logger is configured and emits logs to the following
    two locations:
        - `console`: Logs to the console.
        - `file_train_app_worker`: Ray Train application logs.
    The root logger will not emit Ray Train system logs and thus not writing to
    `file_train_sys_worker` file handler.
    """

    config_dict = _get_base_logger_config_dict(context)
    config_dict["loggers"]["ray.train"] = {
        "level": "INFO",
        "handlers": ["file_train_sys_worker", "file_train_app_worker", "console"],
        "propagate": False,
    }
    config_dict["root"] = {
        "level": "INFO",
        "handlers": ["file_train_app_worker", "console"],
    }
    return config_dict


class TrainContextFilter(logging.Filter):
    """Add Ray Train metadata to the log records.

    This filter is applied to Ray Train controller and worker processes.
    """

    # Log keys for Ray Train controller and worker processes.
    class LogKey(str, Enum):
        RUN_NAME = "run_name"
        COMPONENT = "component"
        WORLD_RANK = "world_rank"
        LOCAL_RANK = "local_rank"
        NODE_RANK = "node_rank"

    # Ray Train Component by process types
    class TrainComponent(str, Enum):
        CONTROLLER = "controller"
        WORKER = "worker"

    def __init__(self, context: TrainRunContext):
        self._run_name: str = context.get_run_config().name
        self._is_worker: bool = isinstance(context, TrainContext)
        if self._is_worker:
            self._world_rank: int = context.get_world_rank()
            self._local_rank: int = context.get_local_rank()
            self._node_rank: int = context.get_node_rank()
            self._component: str = TrainContextFilter.TrainComponent.WORKER
        else:
            self._component: str = TrainContextFilter.TrainComponent.CONTROLLER

    def controller_filter(self, record):
        # Add the run_id and component to Ray Train controller processes.
        setattr(record, TrainContextFilter.LogKey.RUN_NAME, self._run_name)
        setattr(record, TrainContextFilter.LogKey.COMPONENT, self._component)
        return True

    def worker_filter(self, record):
        # Add the run_id and component to Ray Train worker processes.
        setattr(record, TrainContextFilter.LogKey.RUN_NAME, self._run_name)
        setattr(record, TrainContextFilter.LogKey.COMPONENT, self._component)
        # Add all the rank related information to the log record for worker processes.
        setattr(record, TrainContextFilter.LogKey.WORLD_RANK, self._world_rank)
        setattr(record, TrainContextFilter.LogKey.LOCAL_RANK, self._local_rank)
        setattr(record, TrainContextFilter.LogKey.NODE_RANK, self._node_rank)
        return True

    def filter(self, record):
        if self._is_worker:
            return self.worker_filter(record)
        else:
            return self.controller_filter(record)


class SessionFileHandler(logging.Handler):
    """A handler that writes to a log file in the Ray session directory.

    The Ray session directory isn't available until Ray is initialized, so any logs
    emitted before Ray is initialized will be lost.
    This handler will not create the file handler until you emit a log record.

    Args:
        filename: The name of the log file. The file is created in the 'logs/train'
        directory of the Ray session directory.
    """

    # TODO (hpguo): This handler class is shared by both Ray Train and ray data. We
    # should move this to ray core and make it available to both libraries.

    def __init__(self, filename: str):
        super().__init__()
        self._filename = filename
        self._handler = None
        self._formatter = None
        self._path = None

    def emit(self, record):
        if self._handler is None:
            self._try_create_handler()
        if self._handler is not None:
            self._handler.emit(record)

    def setFormatter(self, fmt: logging.Formatter) -> None:
        if self._handler is not None:
            self._handler.setFormatter(fmt)
        self._formatter = fmt

    def _get_log_directory(self, session_dir):
        """Return the directory where Ray Train writes log files."""
        return os.path.join(session_dir, "logs", "train")

    def _try_create_handler(self):
        assert self._handler is None

        # Get the Ray session directory. If not in a Ray session, return.
        # This handler will only be created within a Ray session.
        global_node = ray._private.worker._global_node
        if not global_node:
            return

        # Create the log directory if it doesn't exist.
        log_directory = self._get_log_directory(global_node.get_session_dir_path())
        os.makedirs(log_directory, exist_ok=True)

        # Create the log file.
        self._path = os.path.join(log_directory, self._filename)
        self._handler = logging.FileHandler(self._path)
        if self._formatter is not None:
            self._handler.setFormatter(self._formatter)


def configure_controller_logger(context: TrainRunContext) -> None:
    """
    Configure the logger on the controller process, which is the `ray.train` logger.
    """
    config = get_controller_logger_config_dict(context)
    logging.config.dictConfig(config)


def configure_worker_logger(context: TrainRunContext) -> None:
    """
    Configure the loggers on the worker process, which contains the
    `ray.train` logger and the root logger.
    """
    config = get_worker_logger_config_dict(context)
    logging.config.dictConfig(config)
