from ray.data._internal.execution.execution_callback import (
    ExecutionCallback,
)
from ray.anyscale.data.issue_detection.issue_detector_manager import (
    IssueDetectorManager,
)
from ray.data._internal.execution.streaming_executor import StreamingExecutor


class IssueDetectionExecutionCallback(ExecutionCallback):
    """ExecutionCallback that handles issue detection."""

    def before_execution_starts(self, executor: StreamingExecutor):
        # Initialize issue detector in StreamingExecutor
        executor._issue_detector_manager = IssueDetectorManager(executor)

    def on_execution_step(self, executor: StreamingExecutor):
        # Invoke all issue detectors
        executor._issue_detector_manager.invoke_detectors()
