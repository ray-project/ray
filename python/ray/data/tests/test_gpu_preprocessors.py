import math
from collections import Counter
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ray.data._internal.compute import ActorPoolStrategy
from ray.data.preprocessor import Preprocessor
from ray.data.preprocessors import (
    Chain,
    GPUChain,
    GPUColumnCaster,
    GPUOrdinalEncoder,
    GPUPowerTransformer,
    GPUSimpleImputer,
    GPUStandardScaler,
    OrdinalEncoder,
    PowerTransformer,
    SimpleImputer,
    StandardScaler,
)
from ray.data.preprocessors.gpu._fusion import (
    _FusedGPUCategoricalOrdinalOp,
    _FusedGPUNumericColumnOp,
    _plan_gpu_transform_ops,
)
from ray.data.preprocessors.gpu._runtime import (
    _apply_gpu_preprocessors,
    _GPUPreprocessorOp,
)
from ray.data.preprocessors.gpu.chain import gpu_concurrency


def _require_cudf_with_cuda():
    cudf = pytest.importorskip("cudf")
    cp = pytest.importorskip("cupy")
    try:
        device_count = cp.cuda.runtime.getDeviceCount()
    except Exception as exc:
        pytest.skip(f"CUDA device is not available: {exc}")
    if device_count == 0:
        pytest.skip("CUDA device is not available.")
    return cudf


def test_chain_builds_fused_gpu_chain_from_standard_preprocessors():
    preprocessor = Chain(
        PowerTransformer(columns=["num_0"], power=0, method="yeo-johnson"),
        StandardScaler(columns=["num_0"]),
        SimpleImputer(columns=["num_0"], strategy="constant", fill_value=0.0),
        OrdinalEncoder(columns=["cat_0"]),
    )

    gpu_chain = GPUChain.from_cpu_preprocessors(
        preprocessor.preprocessors,
        batch_size=1024,
        num_gpus_per_worker=1,
        concurrency=2,
        copy_fitted_state=False,
    )

    assert isinstance(gpu_chain, GPUChain)
    assert [type(p) for p in gpu_chain.preprocessors] == [
        GPUPowerTransformer,
        GPUStandardScaler,
        GPUSimpleImputer,
        GPUOrdinalEncoder,
    ]
    assert all(p._batch_size == 1024 for p in gpu_chain.preprocessors)
    assert all(p._concurrency == 2 for p in gpu_chain.preprocessors)


def test_chain_gpu_lowering_preserves_ordinal_min_evidence():
    preprocessor = Chain(
        OrdinalEncoder(columns=["cat_0"], encode_lists=False, min_evidence=750)
    )

    gpu_chain = GPUChain.from_cpu_preprocessors(
        preprocessor.preprocessors,
        batch_size=1024,
        num_gpus_per_worker=1,
        concurrency=2,
        copy_fitted_state=False,
    )

    gpu_encoder = gpu_chain.preprocessors[0]
    assert isinstance(gpu_encoder, GPUOrdinalEncoder)
    assert gpu_encoder.min_evidence == 750


def test_gpu_ordinal_encoder_min_evidence_filters_global_counts():
    encoder = GPUOrdinalEncoder(
        columns=["cat_0"],
        min_evidence=3,
        encoded_value_offset=1,
    )
    partials = pd.DataFrame(
        [
            {"column": "cat_0", "value": "keep", "count": 2},
            {"column": "cat_0", "value": "drop", "count": 2},
            {"column": "cat_0", "value": "keep", "count": 1},
        ]
    )

    encoder._finalize_gpu_fit_stats(partials)

    assert encoder.stats_ == {"unique_values(cat_0)": {"keep": 1}}


@pytest.mark.parametrize("combined", [False, True])
def test_gpu_ordinal_fit_aggregate_rejects_nulls_before_thresholding(combined):
    from ray.data.preprocessors.gpu._aggregates import (
        GPUOrdinalValueCounter,
        GPUPreprocessorFitAggregate,
    )

    values = MagicMock()
    values.isnull.return_value.any.return_value = True
    df = MagicMock()
    df.__getitem__.return_value = values
    if combined:
        aggregate = GPUPreprocessorFitAggregate(
            ordinal_entries=[(0, ("cat",), (), 3)],
            moment_entries=[],
        )
    else:
        aggregate = GPUOrdinalValueCounter(("cat",), prefix=(), min_evidence=3)

    with patch.dict("sys.modules", {"cudf": MagicMock()}):
        with pytest.raises(ValueError, match="contains null values"):
            aggregate.partial_aggregate(
                df,
                aggregate.generated_key_columns(),
                ("accumulator",),
            )


def test_gpu_ordinal_encoder_vectorized_finalize_matches_reference():
    columns = ["cat_0", "cat_1"]
    partials = pd.DataFrame(
        [
            {"column": "cat_0", "value": "b", "count": 2},
            {"column": "cat_0", "value": "a", "count": 1},
            {"column": "cat_0", "value": "a", "count": 3},
            {"column": "cat_0", "value": None, "count": 100},
            {"column": "cat_1", "value": "z", "count": 4},
            {"column": "cat_1", "value": "drop", "count": 3},
            {"column": "other", "value": "ignored", "count": 100},
        ]
    )
    min_evidence = 4
    offset = 1

    counters = {column: Counter() for column in columns}
    for column, value, count in partials.itertuples(index=False, name=None):
        if column in counters and value is not None:
            counters[column][value] += int(count)
    expected = {
        f"unique_values({column})": {
            value: index + offset
            for index, value in enumerate(
                sorted(
                    value
                    for value, count in counters[column].items()
                    if count >= min_evidence
                )
            )
        }
        for column in columns
    }

    encoder = GPUOrdinalEncoder(
        columns=columns,
        min_evidence=min_evidence,
        encoded_value_offset=offset,
    )
    encoder._finalize_gpu_fit_stats(partials)

    assert encoder.stats_ == expected


def test_gpu_ordinal_encoder_vectorized_finalize_empty_partials():
    encoder = GPUOrdinalEncoder(columns=["cat_0", "cat_1"], min_evidence=3)

    encoder._finalize_gpu_fit_stats(pd.DataFrame())

    assert encoder.stats_ == {
        "unique_values(cat_0)": {},
        "unique_values(cat_1)": {},
    }


def test_gpu_ordinal_encoder_fits_from_distributed_filtered_counts():
    from ray.data.context import ShuffleStrategy
    from ray.data.preprocessors.gpu._aggregates import GPUOrdinalValueCounter

    retained = MagicMock()
    retained.iter_batches.return_value = iter(
        [
            pd.DataFrame(
                {
                    "column": ["cat_1", "cat_0", "cat_0"],
                    "value": ["z", "b", "a"],
                    "count": [4, 5, 4],
                }
            )
        ]
    )
    grouped = MagicMock()
    grouped.aggregate.return_value = retained
    dataset = MagicMock()
    dataset.context.shuffle_strategy = ShuffleStrategy.GPU_SHUFFLE

    encoder = GPUOrdinalEncoder(
        columns=["cat_0", "cat_1"], min_evidence=4, encoded_value_offset=1
    )
    with patch(
        "ray.data.grouped_data.GroupedData", return_value=grouped
    ) as constructor:
        encoder._fit_gpu(dataset, ())

    constructor.assert_called_once_with(
        dataset, ["column", "value"], num_partitions=256
    )
    aggregate = grouped.aggregate.call_args.args[0]
    assert isinstance(aggregate, GPUOrdinalValueCounter)
    assert aggregate.min_evidence == 4
    assert encoder.stats_ == {
        "unique_values(cat_0)": {"a": 1, "b": 2},
        "unique_values(cat_1)": {"z": 1},
    }


def test_gpu_ordinal_encoder_requires_gpu_shuffle_for_fit():
    from ray.data.context import ShuffleStrategy

    dataset = MagicMock()
    dataset.context.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    encoder = GPUOrdinalEncoder(columns=["cat_0"])

    with pytest.raises(ValueError, match="GPU_SHUFFLE"):
        encoder._fit_gpu(dataset, ())


def test_gpu_chain_distributed_combined_fit_requires_gpu_shuffle():
    from ray.data.context import ShuffleStrategy

    dataset = MagicMock()
    dataset.context.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    chain = GPUChain(
        GPUStandardScaler(columns=["num"]),
        GPUOrdinalEncoder(columns=["cat"]),
    )

    with pytest.raises(ValueError, match="GPU_SHUFFLE"):
        chain._fit_distributed_combined(dataset)


@pytest.mark.gpu
def test_gpu_ordinal_encoder_distributed_fit_end_to_end():
    _require_cudf_with_cuda()
    pytest.importorskip("rapidsmpf")

    import ray
    from ray.data.context import DataContext, ShuffleStrategy

    started_ray = not ray.is_initialized()
    if started_ray:
        ray.init(num_gpus=1)

    context = DataContext.get_current()
    original_strategy = context.shuffle_strategy
    original_actors = context.gpu_shuffle_num_actors
    context.shuffle_strategy = ShuffleStrategy.GPU_SHUFFLE
    context.gpu_shuffle_num_actors = 1
    try:
        ds = ray.data.from_items(
            [
                {"cat_0": "keep", "cat_1": "x"},
                {"cat_0": "drop", "cat_1": "x"},
                {"cat_0": "keep", "cat_1": "y"},
                {"cat_0": "keep", "cat_1": None},
            ],
            override_num_blocks=2,
        )
        encoder = GPUOrdinalEncoder(
            columns=["cat_0", "cat_1"],
            min_evidence=3,
            encoded_value_offset=1,
            batch_size=2,
            concurrency=1,
        )

        chain = GPUChain(
            GPUColumnCaster(columns=["cat_0", "cat_1"], output_dtype="str"),
            GPUSimpleImputer(
                columns=["cat_0", "cat_1"],
                strategy="constant",
                fill_value="missing",
                output_dtype="str",
            ),
            encoder,
            batch_size=2,
            concurrency=1,
        )

        chain.fit(ds)

        assert encoder.stats_ == {
            "unique_values(cat_0)": {"keep": 1},
            "unique_values(cat_1)": {},
        }
    finally:
        context.shuffle_strategy = original_strategy
        context.gpu_shuffle_num_actors = original_actors
        if started_ray:
            ray.shutdown()


@pytest.mark.gpu
def test_gpu_chain_fits_moments_and_ordinals_in_one_distributed_pass():
    _require_cudf_with_cuda()
    pytest.importorskip("rapidsmpf")

    import ray
    from ray.data.context import DataContext, ShuffleStrategy

    started_ray = not ray.is_initialized()
    if started_ray:
        ray.init(num_gpus=1)

    context = DataContext.get_current()
    original_strategy = context.shuffle_strategy
    original_actors = context.gpu_shuffle_num_actors
    context.shuffle_strategy = ShuffleStrategy.GPU_SHUFFLE
    context.gpu_shuffle_num_actors = 1
    try:
        ds = ray.data.from_items(
            [
                {"num": 0.0, "cat": "a"},
                {"num": 1.0, "cat": "a"},
                {"num": 2.0, "cat": "a"},
                {"num": 3.0, "cat": "b"},
            ],
            override_num_blocks=4,
        )
        scaler = GPUStandardScaler(columns=["num"], batch_size=2, concurrency=1)
        encoder = GPUOrdinalEncoder(
            columns=["cat"],
            min_evidence=2,
            encoded_value_offset=1,
            batch_size=2,
            concurrency=1,
        )
        chain = GPUChain(
            GPUPowerTransformer(columns=["num"], power=0, method="yeo-johnson"),
            scaler,
            GPUColumnCaster(columns=["cat"], output_dtype="str"),
            encoder,
            batch_size=2,
            concurrency=1,
        )

        chain.fit(ds)

        transformed = [math.log1p(value) for value in range(4)]
        expected_mean = sum(transformed) / len(transformed)
        expected_std = math.sqrt(
            sum((value - expected_mean) ** 2 for value in transformed)
            / len(transformed)
        )
        assert scaler.stats_["mean(num)"] == pytest.approx(expected_mean)
        assert scaler.stats_["std(num)"] == pytest.approx(expected_std)
        assert encoder.stats_ == {"unique_values(cat)": {"a": 1}}
    finally:
        context.shuffle_strategy = original_strategy
        context.gpu_shuffle_num_actors = original_actors
        if started_ray:
            ray.shutdown()


def test_chain_gpu_lowering_copies_fitted_standard_stats():
    scaler = StandardScaler(columns=["num_0"])
    scaler.stats_ = {"mean(num_0)": 2.0, "std(num_0)": 4.0}
    scaler._fitted = True

    encoder = OrdinalEncoder(columns=["cat_0"])
    encoder.stats_ = {"unique_values(cat_0)": {"a": 0, "b": 1}}
    encoder._fitted = True

    preprocessor = Chain(scaler, encoder)
    gpu_chain = GPUChain.from_cpu_preprocessors(
        preprocessor.preprocessors,
        batch_size=None,
        num_gpus_per_worker=1,
        concurrency=None,
        copy_fitted_state=True,
    )

    gpu_scaler, gpu_encoder = gpu_chain.preprocessors
    assert gpu_scaler.stats_ == scaler.stats_
    assert gpu_scaler.fit_status() == Preprocessor.FitStatus.FITTED
    assert gpu_encoder.stats_ == encoder.stats_
    assert gpu_encoder.fit_status() == Preprocessor.FitStatus.FITTED


def test_chain_gpu_transform_reuses_fitted_gpu_chain():
    scaler = StandardScaler(columns=["num_0"])
    scaler.stats_ = {"mean(num_0)": 2.0, "std(num_0)": 4.0}
    scaler._fitted = True

    preprocessor = Chain(scaler)
    gpu_chain = MagicMock()
    gpu_chain.transform.return_value = "transformed"
    preprocessor._gpu_chain = gpu_chain
    dataset = MagicMock()

    with patch.object(GPUChain, "from_cpu_preprocessors") as from_cpu_preprocessors:
        result = preprocessor.transform(dataset, accelerator="gpu")

    from_cpu_preprocessors.assert_not_called()
    gpu_chain.transform.assert_called_once_with(
        dataset, batch_size=None, concurrency=None
    )
    assert result == "transformed"


def test_chain_gpu_lowering_rejects_unknown_accelerator():
    with pytest.raises(ValueError, match="Unsupported accelerator"):
        Chain().fit_transform(None, accelerator="tpu")


def test_gpu_concurrency_resolves_num_gpus():
    assert gpu_concurrency(num_gpus=None, concurrency=None) is None
    assert gpu_concurrency(num_gpus=None, concurrency=3) == 3
    assert gpu_concurrency(num_gpus=2, concurrency=None) == 2
    assert gpu_concurrency(num_gpus=2, concurrency=2) == 2

    with pytest.raises(ValueError, match="num_gpus must be positive"):
        gpu_concurrency(num_gpus=0, concurrency=None)
    with pytest.raises(ValueError, match="Specify either num_gpus or concurrency"):
        gpu_concurrency(num_gpus=2, concurrency=3)


def test_gpu_preprocessors_transform_cudf_batch():
    cudf = _require_cudf_with_cuda()

    df = cudf.DataFrame(
        {
            "num_0": [0.0, 1.0, None],
            "num_1": [3.0, 7.0, 11.0],
            "cat_0": ["b", "a", "b"],
        }
    )

    scaler = GPUStandardScaler(
        columns=["num_0", "num_1"],
        output_dtype="float32",
    )
    scaler.stats_ = {
        "mean(num_0)": math.log1p(0.5),
        "std(num_0)": 1.0,
        "mean(num_1)": 7.0,
        "std(num_1)": 4.0,
    }
    scaler._fitted = True

    encoder = GPUOrdinalEncoder(columns=["cat_0"], output_dtype="int32")
    encoder.stats_ = {"unique_values(cat_0)": {"a": 0, "b": 1}}
    encoder._fitted = True

    preprocessor = GPUChain(
        GPUPowerTransformer(columns=["num_0"], power=0, method="yeo-johnson"),
        scaler,
        GPUSimpleImputer(
            columns=["num_0", "num_1"],
            strategy="constant",
            fill_value=0.0,
            output_dtype="float32",
        ),
        encoder,
        GPUColumnCaster(columns=["cat_0"], output_dtype="int32"),
    )

    result = preprocessor.transform_cudf(df).to_pandas()

    assert result["num_0"].dtype == "float32"
    assert result["num_1"].dtype == "float32"
    assert result["cat_0"].dtype == "int32"
    assert result["cat_0"].tolist() == [1, 0, 1]
    assert result["num_0"].isna().sum() == 0


def test_gpu_chain_plans_generic_fused_transform_ops():
    columns = ["num_0", "num_1"]
    scaler = GPUStandardScaler(columns=columns, output_dtype="float32")
    scaler.stats_ = {
        "mean(num_0)": 0.0,
        "std(num_0)": 1.0,
        "mean(num_1)": 0.0,
        "std(num_1)": 1.0,
    }
    scaler._fitted = True

    encoder = GPUOrdinalEncoder(
        columns=["cat_0"],
        unknown_value=0,
        encoded_missing_value=0,
        output_dtype="int32",
        encoded_value_offset=1,
    )
    encoder.stats_ = {"unique_values(cat_0)": {"a": 1, "missing": 2}}
    encoder._fitted = True

    planned = _plan_gpu_transform_ops(
        (
            GPUPowerTransformer(columns=columns, power=0, method="yeo-johnson"),
            scaler,
            GPUSimpleImputer(
                columns=columns,
                strategy="constant",
                fill_value=0.0,
                output_dtype="float32",
            ),
            GPUColumnCaster(columns=["cat_0"], output_dtype="str"),
            GPUSimpleImputer(
                columns=["cat_0"],
                strategy="constant",
                fill_value="missing",
            ),
            encoder,
        )
    )

    assert [type(op) for op in planned] == [
        _FusedGPUNumericColumnOp,
        _FusedGPUCategoricalOrdinalOp,
    ]

    unfused = _plan_gpu_transform_ops(
        (
            GPUPowerTransformer(columns=["num_0"], power=0),
            GPUStandardScaler(columns=columns),
        )
    )

    assert [type(op) for op in unfused] == [
        _GPUPreprocessorOp,
        _GPUPreprocessorOp,
    ]
    assert [type(op._preprocessor) for op in unfused] == [
        GPUPowerTransformer,
        GPUStandardScaler,
    ]


def test_gpu_chain_fused_transform_matches_sequential_cudf_batch():
    cudf = _require_cudf_with_cuda()

    df = cudf.DataFrame(
        {
            "num_0": [0.0, 1.0, None],
            "num_1": [3.0, None, 11.0],
            "cat_0": ["b", None, "z"],
        }
    )

    columns = ["num_0", "num_1"]
    scaler = GPUStandardScaler(columns=columns, output_dtype="float32")
    scaler.stats_ = {
        "mean(num_0)": 0.25,
        "std(num_0)": 2.0,
        "mean(num_1)": 1.5,
        "std(num_1)": 3.0,
    }
    scaler._fitted = True

    encoder = GPUOrdinalEncoder(
        columns=["cat_0"],
        unknown_value=0,
        encoded_missing_value=0,
        output_dtype="int32",
        encoded_value_offset=1,
    )
    encoder.stats_ = {"unique_values(cat_0)": {"b": 1, "missing": 2}}
    encoder._fitted = True

    preprocessor = GPUChain(
        GPUPowerTransformer(columns=columns, power=0, method="yeo-johnson"),
        scaler,
        GPUSimpleImputer(
            columns=columns,
            strategy="constant",
            fill_value=0.0,
            output_dtype="float32",
        ),
        GPUColumnCaster(columns=["cat_0"], output_dtype="str"),
        GPUSimpleImputer(
            columns=["cat_0"],
            strategy="constant",
            fill_value="missing",
        ),
        encoder,
    )

    sequential = _apply_gpu_preprocessors(
        df.copy(deep=True), preprocessor.preprocessors
    ).to_pandas()
    fused = preprocessor.transform_cudf(df.copy(deep=True)).to_pandas()

    pd.testing.assert_frame_equal(sequential, fused, check_exact=False, rtol=1e-6)
    assert fused["num_0"].dtype == "float32"
    assert fused["num_1"].dtype == "float32"
    assert fused["cat_0"].dtype == "int32"


@pytest.mark.parametrize("intermediate_dtype", ["float32", "int32"])
@pytest.mark.parametrize("first_transform", ["power", "scaler"])
def test_gpu_chain_fused_numeric_transform_recasts_intermediate_dtype(
    intermediate_dtype, first_transform
):
    cudf = _require_cudf_with_cuda()

    scaler = GPUStandardScaler(columns=["num"], output_dtype=intermediate_dtype)
    scaler.stats_ = {
        "mean(num)": 0.23456789,
        "std(num)": 1.3456789,
    }
    scaler._fitted = True
    power = GPUPowerTransformer(
        columns=["num"],
        power=0.5,
        method="yeo-johnson",
        output_dtype=intermediate_dtype,
    )

    if first_transform == "power":
        preprocessors = (
            power,
            GPUStandardScaler(columns=["num"]),
        )
        preprocessors[1].stats_ = {
            "mean(num)": 0.123456789,
            "std(num)": 0.987654321,
        }
        preprocessors[1]._fitted = True
    else:
        preprocessors = (
            scaler,
            GPUPowerTransformer(columns=["num"], power=0, method="yeo-johnson"),
        )

    df = cudf.DataFrame({"num": [-0.654321, 0.123456789, 3.987654321]})
    sequential = _apply_gpu_preprocessors(df.copy(deep=True), preprocessors).to_pandas()
    fused = (
        _FusedGPUNumericColumnOp(preprocessors)
        ._transform_cudf(df.copy(deep=True))
        .to_pandas()
    )

    pd.testing.assert_frame_equal(sequential, fused, rtol=1e-12, atol=1e-12)
    assert fused["num"].dtype == sequential["num"].dtype == "float64"


def test_gpu_chain_uses_prewarmed_actor_pool_strategy():
    class SpyDataset:
        def __init__(self):
            self.calls = []

        def map_batches(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            return self

    ds = SpyDataset()
    GPUChain(GPUColumnCaster(columns=["cat_0"], output_dtype="str"))._transform(
        ds,
        batch_size=None,
        concurrency=4,
    )

    compute = ds.calls[0][1]["compute"]

    assert ds.calls[0][1]["batch_format"] == "cudf"
    assert "output_batch_format" not in ds.calls[0][1]
    assert isinstance(compute, ActorPoolStrategy)
    assert compute.min_size == 4
    assert compute.max_size == 4
    assert compute.initial_size == 4
    assert compute.max_tasks_in_flight_per_actor == 2


def test_gpu_ordinal_encoder_unknown_and_missing_values():
    cudf = _require_cudf_with_cuda()

    df = cudf.DataFrame({"cat_0": ["b", None, "z"]})
    encoder = GPUOrdinalEncoder(
        columns=["cat_0"],
        unknown_value=0,
        encoded_missing_value=0,
        output_dtype="int32",
        encoded_value_offset=1,
    )
    encoder.stats_ = {"unique_values(cat_0)": {"a": 1, "b": 2}}
    encoder._fitted = True

    result = GPUChain(encoder).transform_cudf(df).to_pandas()

    assert result["cat_0"].dtype == "int32"
    assert result["cat_0"].tolist() == [2, 0, 0]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
