import logging
import os

from ray.train.v2._internal.execution.callback import WorkerGroupCallback
from ray.train.v2._internal.execution.context import get_train_context
from ray.train.v2._internal.execution.worker_group import WorkerGroup

logger = logging.getLogger(__name__)


class WorkingDirectorySetupCallback(WorkerGroupCallback):
    def after_worker_group_start(self, worker_group: WorkerGroup):
        def chdir_to_working_dir() -> None:
            """Create the local working directory for the experiment."""
            local_working_directory = (
                get_train_context().get_storage().local_working_directory
            )
            os.makedirs(local_working_directory, exist_ok=True)
            logger.debug(
                f"Changing the working directory to: {local_working_directory}"
            )
            os.chdir(local_working_directory)

        worker_group.execute(chdir_to_working_dir)
