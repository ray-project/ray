import sys
from typing import Any, AsyncIterator, Dict, List, Type

import pydantic
import pytest

import ray
from ray.data.llm import build_llm_processor
from ray.llm._internal.batch.processor import vLLMEngineProcessorConfig
from ray.llm._internal.batch.processor.base import (
    Processor,
    ProcessorBuilder,
    ProcessorConfig,
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
            expected_input_keys: List[str],
            factor: int,
        ):
            super().__init__(data_column, expected_input_keys)
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

    class DummyStage(StatefulStage):
        fn: Type[StatefulStageUDF] = DummyStatefulStageUDF
        fn_constructor_kwargs: Dict[str, Any] = {}
        map_batches_kwargs: Dict[str, Any] = dict(concurrency=1)

        def get_required_input_keys(self) -> Dict[str, str]:
            return {"val": "The value to multiply."}

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


# Common dummy classes for testing
class DummyStatefulStageUDF(StatefulStageUDF):
    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        for row in batch:
            yield row


class DummyStage(StatefulStage):
    fn: Type[StatefulStageUDF] = DummyStatefulStageUDF
    fn_constructor_kwargs: Dict[str, Any] = {}
    map_batches_kwargs: Dict[str, Any] = {}


class DummyProcessorConfig(ProcessorConfig):
    pass


def test_builder():
    def build_processor(config: ProcessorConfig) -> Processor:
        stages = [
            DummyStage(
                fn_constructor_kwargs=dict(),
                map_batches_kwargs=dict(concurrency=1),
            )
        ]
        processor = Processor(config, stages)
        return processor

    ProcessorBuilder.register(DummyProcessorConfig, build_processor)

    processor = ProcessorBuilder.build(DummyProcessorConfig(batch_size=64))
    assert isinstance(processor.config, DummyProcessorConfig)
    assert processor.list_stage_names() == ["DummyStage"]
    assert (
        processor.get_stage_by_name("DummyStage").map_batches_kwargs["concurrency"] == 1
    )

    def overrider(name: str, stage: StatefulStage):
        if name.startswith("DummyStage"):
            stage.map_batches_kwargs["concurrency"] = 2

    processor = ProcessorBuilder.build(
        DummyProcessorConfig(batch_size=64),
        override_stage_config_fn=overrider,
    )
    assert processor.list_stage_names() == ["DummyStage"]
    assert (
        processor.get_stage_by_name("DummyStage").map_batches_kwargs["concurrency"] == 2
    )


class TestBuilderKwargsValidation:
    @pytest.fixture
    def build_processor_with_kwargs(self):
        def build_processor_with_kwargs(
            config: ProcessorConfig,
            preprocess=None,
            postprocess=None,
            custom_kwarg=None,
            another_kwarg=None,
        ) -> Processor:
            stages = [
                DummyStage(
                    fn_constructor_kwargs=dict(
                        custom_kwarg=custom_kwarg,
                        another_kwarg=another_kwarg,
                    ),
                    map_batches_kwargs=dict(concurrency=1),
                )
            ]
            processor = Processor(
                config, stages, preprocess=preprocess, postprocess=postprocess
            )
            return processor

        return build_processor_with_kwargs

    @pytest.fixture(autouse=True)
    def clear_registry(self):
        ProcessorBuilder.clear_registry()

    def test_builder_kwargs_passthrough(self, build_processor_with_kwargs):
        ProcessorBuilder.register(DummyProcessorConfig, build_processor_with_kwargs)

        config = DummyProcessorConfig(batch_size=64)
        processor = build_llm_processor(
            config,
            preprocess=lambda row: {"val": row["id"]},
            postprocess=lambda row: {"result": row["val"]},
            builder_kwargs=dict(
                custom_kwarg="test_value",
                another_kwarg=42,
            ),
        )
        assert processor.list_stage_names() == ["DummyStage"]
        stage = processor.get_stage_by_name("DummyStage")
        assert stage.fn_constructor_kwargs["custom_kwarg"] == "test_value"
        assert stage.fn_constructor_kwargs["another_kwarg"] == 42

    def test_unsupported_kwargs(self):
        def build_processor_no_kwargs(
            config: ProcessorConfig,
            preprocess=None,
            postprocess=None,
        ) -> Processor:
            stages = []
            processor = Processor(
                config, stages, preprocess=preprocess, postprocess=postprocess
            )
            return processor

        ProcessorBuilder.register(DummyProcessorConfig, build_processor_no_kwargs)

        config = DummyProcessorConfig(batch_size=64)
        with pytest.raises(TypeError, match="unsupported_kwarg"):
            build_llm_processor(
                config,
                builder_kwargs=dict(unsupported_kwarg="value"),
            )

    @pytest.mark.parametrize("conflicting_key", ["preprocess", "postprocess"])
    def test_error_builder_kwargs_conflict(
        self, conflicting_key, build_processor_with_kwargs
    ):
        ProcessorBuilder.register(DummyProcessorConfig, build_processor_with_kwargs)

        config = DummyProcessorConfig(batch_size=64)
        with pytest.raises(ValueError, match="builder_kwargs cannot contain"):
            build_llm_processor(
                config,
                preprocess=lambda row: {"val": row["id"]},
                builder_kwargs={conflicting_key: lambda row: {"other": row["id"]}},
            )


class TestProcessorConfig:
    def test_valid_concurrency(self):
        config = vLLMEngineProcessorConfig(
            model_source="unsloth/Llama-3.2-1B-Instruct",
            concurrency=(1, 2),
        )
        assert config.concurrency == (1, 2)

        config = vLLMEngineProcessorConfig(
            model_source="unsloth/Llama-3.2-1B-Instruct",
        )
        assert config.concurrency == 1

    def test_invalid_concurrency(self):
        with pytest.raises(pydantic.ValidationError):
            vLLMEngineProcessorConfig(
                model_source="unsloth/Llama-3.2-1B-Instruct",
                concurrency=1.1,
            )

        with pytest.raises(pydantic.ValidationError):
            vLLMEngineProcessorConfig(
                model_source="unsloth/Llama-3.2-1B-Instruct",
                concurrency=[1, 2, 3],
            )

    @pytest.mark.parametrize("n", [1, 2, 10])
    def test_positive_int_not_fail(self, n):
        conf = ProcessorConfig(concurrency=n)
        assert conf.concurrency == n

    def test_positive_int_unusual_not_fail(self):
        assert ProcessorConfig(concurrency="1").concurrency == 1
        assert ProcessorConfig(concurrency=1.0).concurrency == 1
        assert ProcessorConfig(concurrency="1.0").concurrency == 1

    @pytest.mark.parametrize("pair", [(1, 1), (1, 2), (2, 8)])
    def test_valid_tuple_not_fail(self, pair):
        conf = ProcessorConfig(concurrency=pair)
        assert conf.concurrency == pair

    def test_valid_tuple_unusual_not_fail(self):
        assert ProcessorConfig(concurrency=("1", 2)).concurrency == (1, 2)
        assert ProcessorConfig(concurrency=(1, "2")).concurrency == (1, 2)
        assert ProcessorConfig(concurrency=[1, "2"]).concurrency == (1, 2)

    @pytest.mark.parametrize(
        "bad,msg_part",
        [
            (0, "positive integer"),
            (-5, "positive integer"),
            ((1, 2, 3), "at most 2 items"),
            ((0, 1), "positive integers"),
            ((1, 0), "positive integers"),
            ((-1, 2), "positive integers"),
            ((1, -2), "positive integers"),
            ((1, 2.5), "a number with a fractional part"),
            ("2.1", "unable to parse string"),
            ((5, 2), "min > max"),
        ],
    )
    def test_invalid_inputs_raise(self, bad, msg_part):
        with pytest.raises(pydantic.ValidationError) as e:
            ProcessorConfig(concurrency=bad)
        assert msg_part in str(e.value)

    @pytest.mark.parametrize(
        "n,expected", [(1, (1, 1)), (4, (1, 4)), (10, (1, 10)), ("10", (1, 10))]
    )
    def test_with_int_concurrency_scaling(self, n, expected):
        conf = ProcessorConfig(concurrency=n)
        assert conf.get_concurrency() == expected

    @pytest.mark.parametrize("n,expected", [(1, (1, 1)), (4, (4, 4)), (10, (10, 10))])
    def test_with_int_concurrency_fixed(self, n, expected):
        conf = ProcessorConfig(concurrency=n)
        assert conf.get_concurrency(autoscaling_enabled=False) == expected

    @pytest.mark.parametrize("pair", [(1, 1), (1, 3), (2, 8)])
    def test_with_tuple_concurrency(self, pair):
        conf = ProcessorConfig(concurrency=pair)
        assert conf.get_concurrency() == pair


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
