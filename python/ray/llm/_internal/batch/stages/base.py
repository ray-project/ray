"""The base class for all stages."""
import logging
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Type

import pyarrow
from pydantic import BaseModel, Field

from ray.data.block import UserDefinedFunction

logger = logging.getLogger(__name__)


def wrap_preprocess(
    fn: UserDefinedFunction,
    processor_data_column: str,
) -> Callable:
    """Wrap the preprocess function, so that the output schema of the
    preprocess is normalized to {processor_data_column: fn(row), other input columns}.

    Args:
        fn: The function to be applied.
        processor_data_column: The internal data column name of the processor.

    Returns:
        The wrapped function.
    """

    def _preprocess(row: dict[str, Any]) -> dict[str, Any]:
        # First put everything into processor_data_column.
        outputs = {processor_data_column: row}

        # Then apply the preprocess function and add its outputs.
        preprocess_output = fn(row)
        outputs[processor_data_column].update(preprocess_output)
        return outputs

    return _preprocess


def wrap_postprocess(
    fn: UserDefinedFunction,
    processor_data_column: str,
) -> Callable:
    """Wrap the postprocess function to remove the processor_data_column.
    Note that we fully rely on users to determine which columns to carry over.

    Args:
        fn: The function to be applied.
        processor_data_column: The internal data column name of the processor.

    Returns:
        The wrapped function.
    """

    def _postprocess(row: dict[str, Any]) -> dict[str, Any]:
        if processor_data_column not in row:
            raise ValueError(
                f"[Internal] {processor_data_column} not found in row {row}"
            )

        return fn(row[processor_data_column])

    return _postprocess


class StatefulStageUDF:
    """A stage UDF wrapper that processes the input and output columns
    before and after the UDF.

    Args:
        data_column: The internal data column name of the processor. The
                     __call__ method takes the data column as the input of the UDF
                     method, and encapsulates the output of the UDF method into the data
                     column for the next stage.
        expected_input_keys: The expected input keys of the stage.
    """

    # The internal column name for the index of the row in the batch.
    # This is used to align the output of the UDF with the input batch.
    IDX_IN_BATCH_COLUMN: str = "__idx_in_batch"

    def __init__(
        self, data_column: str, expected_input_keys: Optional[List[str]] = None
    ):
        self.data_column = data_column
        self.expected_input_keys = set(expected_input_keys or [])

    async def __call__(self, batch: Dict[str, Any]) -> AsyncIterator[Dict[str, Any]]:
        """A stage UDF wrapper that processes the input and output columns
        before and after the UDF. The expected schema of "batch" is:
        {data_column: {
            dataset columns,
            other intermediate columns
         },
         ...other metadata columns...,
        }.

        The input of the UDF will then [dataset columns and other intermediate columns].
        In addition, while the output of the UDF depends on the UDF implementation,
        the output schema is expected to be
        {data_column: {
            dataset columns,
            other intermediate columns,
            UDF output columns (will override above columns if they have the same name)
         },
         ...other metadata columns...,
        }.
        And this will become the input of the next stage.

        Examples:
            Input dataset columns: {A, B, C}
            Preprocess: (lambda row: {"D": row["A"] + 1})
                Input:
                    UDF input: {A, B, C}
                    UDF output: {D}
                Output: {__data: {A, B, C, D}}
            Stage 1:
                Input: {__data: {A, B, C, D}}
                    UDF input: {A, B, C, D}
                    UDF output: {E}
                Output: {__data: {A, B, C, D, E}}
            Stage 2:
                Input: {__data: {A, B, C, D, E}}
                    UDF input: {A, B, C, D, E}
                    UDF output: {F, E} # E is in-place updated.
                Output: {__data: {A, B, C, D, E, F}}
            Postprocess: (lambda row: {"G": row["F"], "A": row["A"], "E": row["E"]})
                Input: {__data: {A, B, C, D, E, F}}
                    UDF input: {A, B, C, D, E, F}
                    UDF output: {G, A, E}
                Output: {G, A, E} # User chooses to keep G, A, E.

        Args:
            batch: The input batch.

        Returns:
            An async iterator of the outputs.
        """
        # Handle the case where the batch is empty.
        # FIXME: This should not happen.
        if isinstance(batch, pyarrow.lib.Table) and batch.num_rows == 0:
            yield {}
            return

        if self.data_column not in batch:
            raise ValueError(
                f"[Internal] {self.data_column} not found in batch {batch}"
            )

        inputs = batch.pop(self.data_column)
        if hasattr(inputs, "tolist"):
            inputs = inputs.tolist()
        self.validate_inputs(inputs)

        # Assign the index of the row in the batch to the idx_in_batch_column.
        # This is beacuse the UDF output may be out-of-order (if asyncio.as_completed
        # is used interanlly for example), and we need to carry over unused input
        # columns to the next stage. Thus, we use the row index in batch to match
        # the output of the UDF with the input.
        for idx, row in enumerate(inputs):
            row[self.IDX_IN_BATCH_COLUMN] = idx

        # Collect all outputs first, then return them in the original order
        # This is a requirement set by https://github.com/ray-project/ray/pull/54190/
        not_outputed_rows = set(range(len(inputs)))
        async for output in self.udf(inputs):
            if self.IDX_IN_BATCH_COLUMN not in output:
                raise ValueError(
                    "The output of the UDF must contain the column "
                    f"{self.IDX_IN_BATCH_COLUMN}."
                )
            idx_in_batch = output.pop(self.IDX_IN_BATCH_COLUMN)
            if idx_in_batch not in not_outputed_rows:
                raise ValueError(
                    f"The row {idx_in_batch} is outputed twice. "
                    "This is likely due to the UDF is not one-to-one."
                )
            not_outputed_rows.remove(idx_in_batch)

            # Add stage outputs to the data column of the row.
            inputs[idx_in_batch].pop(self.IDX_IN_BATCH_COLUMN)
            inputs[idx_in_batch].update(output)

        if not_outputed_rows:
            raise ValueError(f"The rows {not_outputed_rows} are not outputed.")

        # Return all updated inputs in the original order
        yield {self.data_column: inputs}

    def validate_inputs(self, inputs: List[Dict[str, Any]]):
        """Validate the inputs to make sure the required keys are present.

        Args:
            inputs: The inputs.

        Raises:
            ValueError: If the required keys are not found.
        """
        for inp in inputs:
            input_keys = set(inp.keys())

            if self.IDX_IN_BATCH_COLUMN in input_keys:
                raise ValueError(
                    f"The input column {self.IDX_IN_BATCH_COLUMN} is reserved "
                    "for internal use."
                )

            if not self.expected_input_keys:
                continue

            missing_required = self.expected_input_keys - input_keys
            if missing_required:
                raise ValueError(
                    f"Required input keys {missing_required} not found at the input of "
                    f"{self.__class__.__name__}. Input keys: {input_keys}"
                )

    async def udf(self, rows: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        raise NotImplementedError("StageUDF must implement the udf method")


class StatefulStage(BaseModel):
    """
    A basic building block to compose a Processor.
    """

    fn: Type[StatefulStageUDF] = Field(
        description="The well-optimized stateful UDF for this stage."
    )
    fn_constructor_kwargs: Dict[str, Any] = Field(
        default_factory=dict,
        description="The keyword arguments of the UDF constructor.",
    )
    map_batches_kwargs: Dict[str, Any] = Field(
        default_factory=lambda: {"concurrency": 1},
        description="The arguments of .map_batches(). Default {'concurrency': 1}.",
    )

    def get_required_input_keys(self) -> Dict[str, str]:
        """The required input keys of the stage and their descriptions."""
        return {}

    def get_optional_input_keys(self) -> Dict[str, str]:
        """The optional input keys of the stage and their descriptions."""
        return {}

    def get_dataset_map_batches_kwargs(
        self,
        batch_size: int,
        data_column: str,
    ) -> Dict[str, Any]:
        """We separate fn and fn_constructor_kwargs in Stage for better UX,
        so we combine them with other map_batches_kwargs together in this method.

        Args:
            batch_size: The batch size set by the processor config.
            data_column: The data column name set by the processor.

        Returns:
            The dataset map_batches kwargs.
        """
        kwargs = self.map_batches_kwargs.copy()
        batch_size_in_kwargs = kwargs.get("batch_size", batch_size)
        if batch_size_in_kwargs != batch_size:
            logger.warning(
                "batch_size is set to %d in map_batches_kwargs, but it will be "
                "overridden by the batch size configured by the processor %d.",
                batch_size_in_kwargs,
                batch_size,
            )
        kwargs["batch_size"] = batch_size

        kwargs.update({"fn_constructor_kwargs": self.fn_constructor_kwargs.copy()})
        if "data_column" in kwargs["fn_constructor_kwargs"]:
            raise ValueError(
                "'data_column' cannot be used as in fn_constructor_kwargs."
            )

        kwargs["fn_constructor_kwargs"]["data_column"] = data_column
        kwargs["fn_constructor_kwargs"]["expected_input_keys"] = list(
            self.get_required_input_keys().keys()
        )
        return kwargs

    class Config:
        arbitrary_types_allowed = True
        validate_assignment = True
