import logging
import os

from ray.train.v2._internal.execution.callback import (
    ReplicaGroupCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.context import get_train_context
from ray.train.v2._internal.execution.worker_group import (
    ExecutionGroup,
)

logger = logging.getLogger(__name__)


class WorkingDirectorySetupCallback(ReplicaGroupCallback, WorkerGroupCallback):
    def after_execution_group_start(self, execution_group: ExecutionGroup):
        """Shared logic for setting up the working directory on an execution group."""

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

        execution_group.execute(chdir_to_working_dir)
