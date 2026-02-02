import sys
from typing import Any, AsyncIterator, Dict, List, Optional

import pytest

from ray.llm._internal.batch.stages.base import (
    StatefulStageUDF,
    wrap_postprocess,
    wrap_preprocess,
)


def test_wrap_preprocess():
    # Test function that doubles a number
    def double(x: dict) -> dict:
        return {"value": x["id"] * 2}

    # Test with carry_over=True
    wrapped = wrap_preprocess(double, "__data")
    result = wrapped({"id": 5, "extra": "memo"})
    assert result == {"__data": {"id": 5, "extra": "memo", "value": 10}}


def test_wrap_postprocess():
    # Test function that converts number to string
    def to_string(x: dict) -> dict:
        return {
            "result": str(x["value"]),
            "extra": x["extra"],
        }

    # Test with carry_over=True
    wrapped = wrap_postprocess(to_string, "__data")
    result = wrapped({"__data": {"id": 5, "extra": "memo", "value": 10}})
    assert result == {"extra": "memo", "result": "10"}

    # Test missing input column
    with pytest.raises(ValueError):
        wrapped({"wrong_key": 42})


def test_wrap_postprocess_bypasses_error_rows():
    """Error rows with __inference_error__ set bypass user postprocess."""

    def user_fn(data: dict) -> dict:
        # Would crash if called with error row (missing generated_text)
        return {"response": data["generated_text"].upper()}

    wrapped = wrap_postprocess(user_fn, "__data")

    error_row = {
        "__data": {
            "__inference_error__": "ValueError: prompt too long",
            "prompt": "This is a long prompt",
        }
    }
    result = wrapped(error_row)
    # Error rows return entire data dict to preserve debugging info
    assert result == {
        "__inference_error__": "ValueError: prompt too long",
        "prompt": "This is a long prompt",
    }


def test_wrap_postprocess_success_rows_run_postprocess():
    """Success rows (__inference_error__ is None) run user postprocess."""

    def user_fn(data: dict) -> dict:
        return {"response": data["generated_text"], "tokens": data["num_tokens"]}

    wrapped = wrap_postprocess(user_fn, "__data")

    success_row = {
        "__data": {
            "generated_text": "Hello world",
            "num_tokens": 10,
            "__inference_error__": "",
        }
    }
    result = wrapped(success_row)
    assert result == {"response": "Hello world", "tokens": 10}


def test_wrap_postprocess_include_error_column():
    """With include_error_column=True, success rows include __inference_error__: None."""

    def user_fn(data: dict) -> dict:
        return {"response": data["generated_text"]}

    wrapped = wrap_postprocess(user_fn, "__data", include_error_column=True)

    success_row = {
        "__data": {
            "generated_text": "Hello world",
            "__inference_error__": "",
        }
    }
    result = wrapped(success_row)
    assert result == {"response": "Hello world", "__inference_error__": ""}

    # Error rows return entire data dict to preserve debugging info
    error_row = {
        "__data": {
            "__inference_error__": "ValueError: prompt too long",
            "prompt": "a long prompt",
        }
    }
    result = wrapped(error_row)
    assert result == {
        "__inference_error__": "ValueError: prompt too long",
        "prompt": "a long prompt",
    }


class TestStatefulStageUDF:
    class SimpleUDF(StatefulStageUDF):
        def __init__(
            self,
            data_column: str,
            expected_input_keys: Optional[List[str]] = None,
            udf_output_missing_idx_in_batch_column: bool = False,
        ):
            super().__init__(data_column, expected_input_keys)
            self.udf_output_missing_idx_in_batch_column = (
                udf_output_missing_idx_in_batch_column
            )

        async def udf(
            self, rows: list[Dict[str, Any]]
        ) -> AsyncIterator[Dict[str, Any]]:

            # Intentionally output in a reversed order to test OOO.
            for row in rows[::-1]:
                ret = {"processed": row["value"] * 2}
                if not self.udf_output_missing_idx_in_batch_column:
                    ret[self.IDX_IN_BATCH_COLUMN] = row[self.IDX_IN_BATCH_COLUMN]
                yield ret

    @pytest.mark.asyncio
    async def test_basic_processing(self):
        udf = self.SimpleUDF(data_column="__data", expected_input_keys=["value"])

        batch = {
            "__data": [{"value": 1, "extra": 10}, {"value": 2, "extra": 20}],
        }

        results = []
        async for result in udf(batch):
            results.extend(result["__data"])

        assert len(results) == 2
        for data in results:
            val = data["value"]
            assert data["processed"] == val * 2
            assert data["extra"] == 10 * val
            assert data["value"] == val

    @pytest.mark.asyncio
    async def test_missing_data_column(self):
        udf = self.SimpleUDF(data_column="__data", expected_input_keys=["value"])

        batch = {"extra": ["a"]}

        with pytest.raises(ValueError):
            async for _ in udf(batch):
                pass

    @pytest.mark.asyncio
    async def test_missing_required_key(self):
        udf = self.SimpleUDF(data_column="__data", expected_input_keys=["value"])

        batch = {"__data": [{"wrong_key": 1}]}

        with pytest.raises(ValueError):
            async for _ in udf(batch):
                pass

    @pytest.mark.asyncio
    async def test_missing_idx_in_batch_column(self):
        udf = self.SimpleUDF(
            data_column="__data",
            expected_input_keys=["value"],
            udf_output_missing_idx_in_batch_column=True,
        )

        batch = {"__data": [{"value": 1}]}

        with pytest.raises(ValueError):
            async for _ in udf(batch):
                pass

    @pytest.mark.asyncio
    async def test_error_rows_bypass_udf(self):
        """Error rows with __inference_error__ bypass the UDF entirely."""

        class FailOnMissingValueUDF(StatefulStageUDF):
            async def udf(
                self, rows: list[Dict[str, Any]]
            ) -> AsyncIterator[Dict[str, Any]]:
                for row in rows:
                    # Would crash on error rows missing 'value' field
                    yield {
                        self.IDX_IN_BATCH_COLUMN: row[self.IDX_IN_BATCH_COLUMN],
                        "processed": row["value"] * 2,
                    }

        udf = FailOnMissingValueUDF(
            data_column="__data", expected_input_keys=None  # Skip validation
        )

        batch = {
            "__data": [
                {"value": 1},  # Normal row
                {"__inference_error__": "ValueError: prompt too long"},  # Error row
                {"value": 3},  # Normal row
            ]
        }

        results = []
        async for result in udf(batch):
            results.extend(result["__data"])

        assert len(results) == 3

        # Normal rows are processed
        assert results[0]["processed"] == 2
        assert results[2]["processed"] == 6

        # Error row passes through unchanged
        assert results[1]["__inference_error__"] == "ValueError: prompt too long"
        assert "processed" not in results[1]

    @pytest.mark.asyncio
    async def test_all_error_rows_in_batch(self):
        """Batch with all error rows should pass through without calling UDF."""

        class FailIfCalledUDF(StatefulStageUDF):
            async def udf(
                self, rows: list[Dict[str, Any]]
            ) -> AsyncIterator[Dict[str, Any]]:
                raise AssertionError("UDF should not be called for all-error batch")
                yield  # Make this a generator

        udf = FailIfCalledUDF(data_column="__data", expected_input_keys=None)

        batch = {
            "__data": [
                {"__inference_error__": "error 1"},
                {"__inference_error__": "error 2"},
            ]
        }

        results = []
        async for result in udf(batch):
            results.extend(result["__data"])

        assert len(results) == 2
        assert results[0]["__inference_error__"] == "error 1"
        assert results[1]["__inference_error__"] == "error 2"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
