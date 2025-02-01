"""The base class for all stages."""
import logging
from typing import Any, Dict, AsyncIterator, List, Callable

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
        __call__ method will take the data column as the input of the udf
        method, and encapsulate the output of the udf method into the data
        column for the next stage.
    """

    # The internal column name for the index of the row in the batch.
    # This is used to align the output of the UDF with the input batch.
    idx_in_batch_column: str = "__idx_in_batch"

    def __init__(self, data_column: str):
        self.data_column = data_column

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
        for idx, row in enumerate(inputs):
            row[self.idx_in_batch_column] = idx

        # Always stream the outputs one by one to better overlapping
        # batches. For example, when the output batch size is 64, Ray Data
        # will collect 64 outputs, and 1) send the batch of 64 to the next stage,
        # 2) get the next batch of this stage. Assuming the input batch size
        # is 63 and we yield all 63 results at once, then Ray Data will wait
        # for 2 batches (63 + 63 > 64) to continue proceeding. On the other hand,
        # if we stream outputs one-by-one, Ray Data can form a batch of 64 before
        # the second batch is done.
        is_outputed = [False] * len(inputs)
        async for output in self.udf(inputs):
            if self.idx_in_batch_column not in output:
                raise ValueError(
                    "The output of the UDF must contain the column "
                    f"{self.idx_in_batch_column}."
                )
            idx_in_batch = output.pop(self.idx_in_batch_column)
            if is_outputed[idx_in_batch]:
                raise ValueError(
                    f"The row {idx_in_batch} is outputed twice. "
                    "This is likely due to the UDF is not one-to-one."
                )
            is_outputed[idx_in_batch] = True

            # Add stage outputs to the data column of the row.
            inputs[idx_in_batch].pop(self.idx_in_batch_column)
            inputs[idx_in_batch].update(output)
            yield {self.data_column: [inputs[idx_in_batch]]}

        if not all(is_outputed):
            missing_rows = [i for i, o in enumerate(is_outputed) if not o]
            raise ValueError(f"The rows {missing_rows} are not outputed.")

    def validate_inputs(self, inputs: List[Dict[str, Any]]):
        """Validate the inputs to make sure the required keys are present.

        Args:
            inputs: The inputs.

        Raises:
            ValueError: If the required keys are not found.
        """
        input_keys = set(inputs[0].keys())

        if self.idx_in_batch_column in input_keys:
            raise ValueError(
                f"The input column {self.idx_in_batch_column} is reserved "
                "for internal use."
            )

        expected_input_keys = self.expected_input_keys
        if not expected_input_keys:
            return

        missing_required = set(expected_input_keys) - input_keys
        if missing_required:
            raise ValueError(
                f"Required input keys {missing_required} not found at the input of "
                f"{self.__class__.__name__}. Input keys: {input_keys}"
            )

    @property
    def expected_input_keys(self) -> List[str]:
        """A list of required input keys. Missing required keys will raise
        an exception.

        Returns:
            A list of required input keys.
        """
        return []

    async def udf(self, rows: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        raise NotImplementedError("StageUDF must implement the udf method")


class StatefulStage(BaseModel):
    """
    A basic building block to compose a Processor.
    """

    fn: StatefulStageUDF = Field(
        description="The well-optimized stateful UDF for this stage."
    )
    fn_constructor_kwargs: Dict[str, Any] = Field(
        description="The keyword arguments of the UDF constructor."
    )
    map_batches_kwargs: Dict[str, Any] = Field(
        description="The arguments of .map_batches()."
    )

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

        kwargs.update({"fn_constructor_kwargs": self.fn_constructor_kwargs})
        if "data_column" in kwargs["fn_constructor_kwargs"]:
            raise ValueError(
                "'data_column' cannot be used as in fn_constructor_kwargs."
            )

        kwargs["fn_constructor_kwargs"]["data_column"] = data_column
        return kwargs

    class Config:
        arbitrary_types_allowed = True
        validate_assignment = True
