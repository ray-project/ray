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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
