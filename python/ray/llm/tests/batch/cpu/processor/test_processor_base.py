import sys
from typing import Any, AsyncIterator, Dict, List, Type

import pytest

import ray
from ray.llm._internal.batch.processor.base import (
    Processor,
    ProcessorConfig,
    ProcessorBuilder,
)
from ray.llm._internal.batch.stages.base import StatefulStage, StatefulStageUDF


def test_empty_processor():
    """Test processor with only preprocess and postprocess."""

    processor = Processor(
        config=ProcessorConfig(
            batch_size=64,
            accelerator_type=None,
            concurrency=1,
        ),
        stages=[],
        # {id} -> {__data: {id, val}}
        preprocess=lambda row: {"val": row["id"] + 5},
        # {__data: {id, val}} -> {id, result}
        postprocess=lambda row: {"result": row["val"], "id": row["id"]},
    )

    ds = ray.data.range(5)
    ds = processor(ds).take_all()
    for row in ds:
        assert "val" not in row
        assert "id" in row
        assert "result" in row


def test_processor_with_no_preprocess_or_postprocess():
    """Test processor with no preprocess or postprocess."""

    processor = Processor(
        config=ProcessorConfig(
            batch_size=64,
            accelerator_type=None,
            concurrency=1,
        ),
        stages=[],
    )

    ds = ray.data.range(5)
    ds = processor(ds).take_all()
    for row in ds:
        assert "id" in row


@pytest.mark.parametrize("has_extra", [True, False])
def test_processor_with_stages(has_extra: bool):
    """Test processor with multiple stages."""

    class DummyStatefulStageUDF(StatefulStageUDF):
        def __init__(
            self,
            data_column: str,
            factor: int,
        ):
            super().__init__(data_column)
            self.factor = factor

        async def udf(
            self, batch: List[Dict[str, Any]]
        ) -> AsyncIterator[Dict[str, Any]]:
            for row in batch:
                answer = row["val"] * self.factor
                if "extra" in row:  # Optional input column.
                    answer += row["extra"]
                yield {
                    # Use the same name to chain multiple dummy stages.
                    "val": answer,
                    self.IDX_IN_BATCH_COLUMN: row[self.IDX_IN_BATCH_COLUMN],
                }

        @property
        def expected_input_keys(self) -> List[str]:
            return ["val"]

    class DummyStage(StatefulStage):
        fn: Type[StatefulStageUDF] = DummyStatefulStageUDF
        fn_constructor_kwargs: Dict[str, Any] = {}
        map_batches_kwargs: Dict[str, Any] = dict(concurrency=1)

    stages = [
        DummyStage(fn_constructor_kwargs=dict(factor=2)),
        DummyStage(fn_constructor_kwargs=dict(factor=3)),
    ]

    processor = Processor(
        config=ProcessorConfig(
            accelerator_type=None,
            concurrency=1,
            batch_size=64,
        ),
        stages=stages,
        preprocess=lambda row: {"val": row["id"]},
        postprocess=lambda row: {"result": row["val"], "id": row["id"]},
    )

    # Check the stage names.
    stage_names = processor.list_stage_names()
    assert stage_names == [
        "DummyStage",
        "DummyStage_1",
    ]

    # Check the stages.
    for stage_name, stage in zip(stage_names, stages):
        assert processor.get_stage_by_name(stage_name) == stage

    # Run the processor twice with different datasets to test
    # whether the processor is reusable.
    for _ in range(2):
        ds = ray.data.range(5)
        ds = ds.map(
            lambda row: {
                "id": row["id"],
                **({"extra": 1} if has_extra else {}),
            }
        )

        ds = processor(ds).take_all()
        extra = 1 if has_extra else 0
        for row in ds:
            assert "id" in row
            assert "result" in row

            # The final output should be the result of the last stage.
            assert row["result"] == (row["id"] * 2 + extra) * 3 + extra


def test_builder():
    class DummyStatefulStageUDF(StatefulStageUDF):
        async def udf(
            self, batch: List[Dict[str, Any]]
        ) -> AsyncIterator[Dict[str, Any]]:
            for row in batch:
                yield row

    class DummyStage(StatefulStage):
        fn: Type[StatefulStageUDF] = DummyStatefulStageUDF
        fn_constructor_kwargs: Dict[str, Any] = {}
        map_batches_kwargs: Dict[str, Any] = {}

    class TestBuilderDummyProcessorConfig(ProcessorConfig):
        pass

    def build_processor(config: ProcessorConfig) -> Processor:
        stages = [
            DummyStage(
                fn_constructor_kwargs=dict(),
                map_batches_kwargs=dict(concurrency=1),
            )
        ]
        processor = Processor(config, stages)
        return processor

    ProcessorBuilder.register(TestBuilderDummyProcessorConfig, build_processor)

    processor = ProcessorBuilder.build(TestBuilderDummyProcessorConfig(batch_size=64))
    assert isinstance(processor.config, TestBuilderDummyProcessorConfig)
    assert processor.list_stage_names() == ["DummyStage"]
    assert (
        processor.get_stage_by_name("DummyStage").map_batches_kwargs["concurrency"] == 1
    )

    def overrider(name: str, stage: StatefulStage):
        if name.startswith("DummyStage"):
            stage.map_batches_kwargs["concurrency"] = 2

    processor = ProcessorBuilder.build(
        TestBuilderDummyProcessorConfig(batch_size=64),
        override_stage_config_fn=overrider,
    )
    assert processor.list_stage_names() == ["DummyStage"]
    assert (
        processor.get_stage_by_name("DummyStage").map_batches_kwargs["concurrency"] == 2
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
