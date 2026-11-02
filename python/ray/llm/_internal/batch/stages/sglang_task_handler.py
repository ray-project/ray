"""The per-task interface of the SGLang engine stage."""

from typing import Any, ClassVar, Dict

from pydantic import BaseModel


class _SGLangRequest(BaseModel):
    """The fields every task's engine request must have."""

    # The request ID for the LLM engine (unique per replica).
    request_id: int
    # The index of the request in the batch.
    idx_in_batch: int


class _SGLangTaskHandler:
    """The per-task part of the SGLang engine stage.

    Each task type has one subclass, registered in
    `sglang_engine_stage._TASK_HANDLERS`. The subclass defines the task's
    input columns, adjusts the engine kwargs, turns rows into requests, and
    runs the requests on the engine. The engine stage handles everything
    else: engine creation, request IDs, concurrency limits, and batching.
    """

    # The input columns the task requires, and their descriptions.
    required_input_keys: ClassVar[Dict[str, str]] = {}
    # The input columns the task can use, and their descriptions.
    optional_input_keys: ClassVar[Dict[str, str]] = {}

    def configure_engine_kwargs(self, engine_kwargs: Dict[str, Any]) -> None:
        """Adjust the `sglang.Engine` kwargs in place for this task."""

    def check_engine(self, engine_cls: Any) -> None:
        """Raise if `engine_cls` can't run this task, before the engine loads."""

    def prepare_request(
        self, row: Dict[str, Any], request_id: int, idx_in_batch: int
    ) -> _SGLangRequest:
        """Build the engine request for one row."""
        raise NotImplementedError

    async def run(self, engine: Any, request: _SGLangRequest) -> Dict[str, Any]:
        """Run one request on the engine and return its output columns."""
        raise NotImplementedError
