"""TBA"""
import logging
from enum import Enum, auto
from typing import Any, Dict, AsyncIterator, List, Callable, Tuple

import pyarrow
from pydantic import BaseModel
from ray.data.block import UserDefinedFunction

logger = logging.getLogger(__name__)


def wrap_preprocess(
    fn: UserDefinedFunction,
    input_column: str,
    carry_over: bool,
) -> Callable:
    """Wrap the preprocess function, so that the output schema of the
    preprocess is normalized to {input_column: ...} (with other input columns
    if carry_over is True).

    Args:
        fn: The function to be applied.
        input_column: The input column name.
        carry_over: Whether to carry over the output column to the next stage.

    Returns:
        The wrapped function.
    """

    def _preprocess(row: dict[str, Any]) -> dict[str, Any]:
        outputs = {input_column: fn(row)}

        if carry_over:
            if input_column in row:
                row.pop(input_column)
            outputs.update(row)
        return outputs

    return _preprocess


def wrap_postprocess(
    fn: UserDefinedFunction,
    input_column: str,
    carry_over: bool,
) -> Callable:
    """Wrap the postprocess function, so that the output schema of the
    postprocess is the columns of the last stage plus carry over columns.

    Args:
        fn: The function to be applied.
        input_column: The input column name.
        carry_over: Whether to carry over the output column to the next stage.

    Returns:
        The wrapped function.
    """

    def _postprocess(row: dict[str, Any]) -> dict[str, Any]:
        """Extract the input column and apply the function, and wrap
        the outputs with the output column name.
        """
        if input_column not in row:
            raise ValueError(f"Input column {input_column} not found in row {row}")

        # Use .pop() to avoid carrying over the input column.
        inputs = row.pop(input_column)
        outputs = fn(inputs)
        if carry_over:
            outputs.update(row)
        return outputs

    return _postprocess


class StatefulStageUDF:
    class InputKeyType(Enum):
        REQUIRED = auto()
        OPTIONAL = auto()

    def __init__(
        self,
        input_column: str,
        output_column: str,
        carry_over: bool,
    ):
        self.input_column = input_column
        self.output_column = output_column
        self.carry_over = carry_over

    async def __call__(self, batch: Dict[str, Any]) -> AsyncIterator[Dict[str, Any]]:
        # Handle the case where all rows are checkpointed or the batch is empty.
        # When all rows are checkpointed, we will still get a batch in pyarrow.Table
        # format with empty rows. This is a bug and is being tracked here:
        # https://github.com/anyscale/rayturbo/issues/1292
        if isinstance(batch, pyarrow.lib.Table) and batch.num_rows > 0:
            yield {}
            return

        if self.input_column not in batch:
            raise ValueError(
                f"Input column {self.input_column} not found in batch {batch}"
            )

        inputs, rows_wo_inputs = self.make_input_rows(batch)

        idx = 0
        async for output in self.udf(inputs):
            yield {
                self.output_column: [output],
                **{k: [v] for k, v in rows_wo_inputs[idx].items()},
            }
            idx += 1

    def make_input_rows(
        self, batch: Dict[str, Any]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Make the input rows and the rows without the input column.

        Args:
            batch: The batch.

        Returns:
            A tuple of (inputs, rows_wo_inputs).
        """

        # Use .pop() to first remove input column from the batch.
        inputs = batch.pop(self.input_column).tolist()
        input_keys = set(inputs[0].keys())
        batch_size = len(inputs)

        # Transpose the rest fields of the batch to be row-major.
        other_keys = list(batch.keys())
        rows_wo_inputs = [dict(zip(other_keys, v)) for v in zip(*batch.values())]

        # Expected input keys may come from all_inputs or rows_wo_inputs.
        expected_input_keys = self.expected_input_keys
        if expected_input_keys:
            # Validate required keys exist.
            required_keys = {
                k
                for k, v in expected_input_keys.items()
                if v == self.InputKeyType.REQUIRED
            }
            missing_required = required_keys - input_keys
            if missing_required:
                raise ValueError(
                    f"Required input keys {missing_required} not found in batch {batch}"
                )

            move_from_input_to_other = input_keys - expected_input_keys.keys()
            move_from_other_to_input = set(other_keys) & expected_input_keys.keys()
            for inp, other in zip(inputs, rows_wo_inputs):
                for key in move_from_other_to_input:
                    # Do not override input if the column name is conflicted.
                    inp.setdefault(key, other.pop(key))

            # Remove all other keys if carry_over is False.
            if not self.carry_over:
                rows_wo_inputs = [{} for _ in range(batch_size)]

            # Not expected input keys will be carried over regardless carry_over
            # is True or False, because these keys are generated by previous stages
            # and may be used by future stages.
            for other, inp in zip(rows_wo_inputs, inputs):
                for key in move_from_input_to_other:
                    other[key] = inp.pop(key)

        return inputs, rows_wo_inputs

    @property
    def expected_input_keys(self) -> Dict[str, InputKeyType]:
        """The expected input keys. If empty, all keys in the input column
        will be used.

        Returns:
            A dictionary with the expected input keys and their type.
        """
        return {}

    async def udf(self, rows: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        raise NotImplementedError("StageUDF must implement the udf method")


class StatefulStage(BaseModel):
    """
    A basic building block to compose a Processor.
    """

    # A well-optimized stateful UDF for this stage.
    fn: StatefulStageUDF
    # The keyword arguments of the UDF constructor.
    fn_constructor_kwargs: Dict[str, Any]
    # The arguments of .map_batches().
    map_batches_kwargs: Dict[str, Any]

    class Config:
        arbitrary_types_allowed = True
        validate_assignment = True
