import pandas as pd
import pytest

import ray
from ray.data.preprocessor import Preprocessor
from ray.data.preprocessors import Chain, LabelEncoder, SimpleImputer, StandardScaler
from ray.data.util.data_batch_conversion import BatchFormat


def test_chain():
    """Tests basic Chain functionality."""
    col_a = [-1, -1, 1, 1]
    col_b = [1, 1, 1, None]
    col_c = ["sunday", "monday", "tuesday", "tuesday"]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    imputer = SimpleImputer(["B"])
    scaler = StandardScaler(["A", "B"])
    encoder = LabelEncoder("C")
    chain = Chain(scaler, imputer, encoder)

    # Fit data.
    chain.fit(ds)
    # Transform data.
    transformed = chain.transform(ds)
    out_df = transformed.to_pandas()

    assert imputer.stats_ == {
        "mean(B)": 0.0,
    }
    assert scaler.stats_ == {
        "mean(A)": 0.0,
        "mean(B)": 1.0,
        "std(A)": 1.0,
        "std(B)": 0.0,
    }
    assert encoder.stats_ == {
        "unique_values(C)": {"monday": 0, "sunday": 1, "tuesday": 2}
    }

    processed_col_a = [-1.0, -1.0, 1.0, 1.0]
    processed_col_b = [0.0, 0.0, 0.0, 0.0]
    processed_col_c = [1, 0, 2, 2]
    expected_df = pd.DataFrame.from_dict(
        {"A": processed_col_a, "B": processed_col_b, "C": processed_col_c}
    ).astype(out_df.dtypes.to_dict())

    pd.testing.assert_frame_equal(out_df, expected_df, check_like=True)

    # Transform batch.
    pred_col_a = [1, 2, None]
    pred_col_b = [0, None, 2]
    pred_col_c = ["monday", "tuesday", "wednesday"]
    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c}
    )

    pred_out_df = chain.transform_batch(pred_in_df)

    pred_processed_col_a = [1, 2, None]
    pred_processed_col_b = [-1.0, 0.0, 1.0]
    pred_processed_col_c = [0, 2, None]
    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_processed_col_a,
            "B": pred_processed_col_b,
            "C": pred_processed_col_c,
        }
    ).astype(pred_out_df.dtypes.to_dict())

    pd.testing.assert_frame_equal(pred_out_df, pred_expected_df, check_like=True)


def test_nested_chain_state():
    col_a = [-1, -1, 1, 1]
    col_b = [1, 1, 1, None]
    col_c = ["sunday", "monday", "tuesday", "tuesday"]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    def create_chain():
        imputer = SimpleImputer(["B"])
        scaler = StandardScaler(["A", "B"])
        encoder = LabelEncoder("C")
        return Chain(Chain(scaler, imputer), encoder)

    chain = create_chain()
    assert chain.fit_status() == Preprocessor.FitStatus.NOT_FITTED

    chain = create_chain()
    chain.preprocessors[1].fit(ds)
    assert chain.fit_status() == Preprocessor.FitStatus.PARTIALLY_FITTED

    chain = create_chain()
    chain.preprocessors[0].fit(ds)
    assert chain.fit_status() == Preprocessor.FitStatus.PARTIALLY_FITTED

    chain.preprocessors[1].fit(ds)
    assert chain.fit_status() == Preprocessor.FitStatus.FITTED

    chain = create_chain()
    chain.fit(ds)
    assert chain.fit_status() == Preprocessor.FitStatus.FITTED


def test_nested_chain():
    """Tests Chain-inside-Chain functionality."""
    col_a = [-1, -1, 1, 1]
    col_b = [1, 1, 1, None]
    col_c = ["sunday", "monday", "tuesday", "tuesday"]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    imputer = SimpleImputer(["B"])
    scaler = StandardScaler(["A", "B"])
    encoder = LabelEncoder("C")
    chain = Chain(Chain(scaler, imputer), encoder)

    # Fit data.
    chain.fit(ds)
    # Transform data.
    transformed = chain.transform(ds)
    out_df = transformed.to_pandas()

    assert imputer.stats_ == {
        "mean(B)": 0.0,
    }
    assert scaler.stats_ == {
        "mean(A)": 0.0,
        "mean(B)": 1.0,
        "std(A)": 1.0,
        "std(B)": 0.0,
    }
    assert encoder.stats_ == {
        "unique_values(C)": {"monday": 0, "sunday": 1, "tuesday": 2}
    }

    processed_col_a = [-1.0, -1.0, 1.0, 1.0]
    processed_col_b = [0.0, 0.0, 0.0, 0.0]
    processed_col_c = [1, 0, 2, 2]
    expected_df = pd.DataFrame.from_dict(
        {"A": processed_col_a, "B": processed_col_b, "C": processed_col_c}
    ).astype(out_df.dtypes.to_dict())

    pd.testing.assert_frame_equal(out_df, expected_df, check_like=True)

    # Transform batch.
    pred_col_a = [1, 2, None]
    pred_col_b = [0, None, 2]
    pred_col_c = ["monday", "tuesday", "wednesday"]
    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c}
    )

    pred_out_df = chain.transform_batch(pred_in_df)

    pred_processed_col_a = [1, 2, None]
    pred_processed_col_b = [-1.0, 0.0, 1.0]
    pred_processed_col_c = [0, 2, None]
    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_processed_col_a,
            "B": pred_processed_col_b,
            "C": pred_processed_col_c,
        }
    ).astype(pred_out_df.dtypes.to_dict())

    pd.testing.assert_frame_equal(pred_out_df, pred_expected_df, check_like=True)


class PreprocessorWithoutTransform(Preprocessor):
    pass


def test_determine_transform_to_use():
    # Test that _determine_transform_to_use doesn't throw any exceptions
    # and selects the transform function of the underlying preprocessor
    # while dealing with the nested Chain case.

    # Check that error is propagated correctly
    with pytest.raises(NotImplementedError):
        chain = Chain(PreprocessorWithoutTransform())
        chain._determine_transform_to_use()

    # Should have no errors from here on
    preprocessor = SimpleImputer(["A"])
    chain1 = Chain(preprocessor)
    format1 = chain1._determine_transform_to_use()
    assert format1 == BatchFormat.PANDAS

    chain2 = Chain(chain1)
    format2 = chain2._determine_transform_to_use()

    assert format1 == format2


def test_chain_serialization():
    """Test Chain serialization and deserialization functionality."""
    import ray
    from ray.data.preprocessor import SerializablePreprocessorBase
    from ray.data.preprocessors import Normalizer, StandardScaler

    # Create and fit chain
    scaler = StandardScaler(columns=["A"])
    normalizer = Normalizer(columns=["A"])
    chain = Chain(scaler, normalizer)

    df = pd.DataFrame({"A": [1.0, 2.0, 3.0]})
    ds = ray.data.from_pandas(df)
    fitted_chain = chain.fit(ds)

    # Serialize using CloudPickle
    serialized = fitted_chain.serialize()

    # Verify it's binary CloudPickle format
    assert isinstance(serialized, bytes)
    assert serialized.startswith(SerializablePreprocessorBase.MAGIC_CLOUDPICKLE)

    # Deserialize
    deserialized = Chain.deserialize(serialized)

    # Verify type and field values
    assert isinstance(deserialized, Chain)
    assert len(deserialized._preprocessors) == 2
    assert isinstance(deserialized._preprocessors[0], StandardScaler)
    assert isinstance(deserialized._preprocessors[1], Normalizer)
    # Verify the StandardScaler is fitted (Normalizer is stateless)
    assert deserialized._preprocessors[0]._fitted

    # Verify it works correctly
    test_df = pd.DataFrame({"A": [1.5, 2.5]})
    result = deserialized.transform_batch(test_df)

    # Result should have been transformed by both preprocessors
    assert "A" in result.columns
    assert len(result) == 2


# ---------------------------------------------------------------------------
# Deferred (aggregation-based) chain fitting
# ---------------------------------------------------------------------------


@pytest.fixture
def deferred_fit_enabled():
    ctx = ray.data.DataContext.get_current()
    original = ctx.enable_aggregation_based_preprocessors
    ctx.enable_aggregation_based_preprocessors = True
    yield
    ctx.enable_aggregation_based_preprocessors = original


@pytest.fixture
def counted_aggregations(monkeypatch):
    """Count the aggregation queries the chain's materialization runs."""
    import ray.data.preprocessors.chain as chain_module

    calls = []
    original = chain_module.execute_aggregate_specs

    def counting(dataset, specs):
        calls.append(len(specs))
        return original(dataset, specs)

    monkeypatch.setattr(chain_module, "execute_aggregate_specs", counting)
    return calls


def _example_dataset():
    return ray.data.from_pandas(
        pd.DataFrame.from_dict(
            {
                "A": [-1, -1, 1, 1],
                "B": [1, 1, 1, None],
                "C": ["sunday", "monday", "tuesday", "tuesday"],
            }
        )
    )


def _example_chain():
    # scaler writes B; imputer reads B -> the imputer's statistic must be
    # computed over the *scaled* B, exactly as sequential fitting does.
    imputer = SimpleImputer(["B"])
    scaler = StandardScaler(["A", "B"])
    encoder = LabelEncoder("C")
    return Chain(scaler, imputer, encoder), (scaler, imputer, encoder)


def test_deferred_fit_computes_stats_at_first_transform(deferred_fit_enabled):
    ds = _example_dataset()
    chain, (scaler, imputer, encoder) = _example_chain()

    chain.fit(ds)
    assert not scaler.has_stats(), "fit should defer statistics computation"
    assert not imputer.has_stats(), "fit should defer statistics computation"
    assert not encoder.has_stats(), "fit should defer statistics computation"
    # The chain still reports itself as fitted; transform is allowed.
    assert chain.fit_status() == Preprocessor.FitStatus.FITTED

    out_df = chain.transform(ds).to_pandas()

    assert scaler.has_stats()
    assert imputer.has_stats()
    assert encoder.has_stats()

    # Same statistics sequential fitting produces (see test_chain above).
    assert imputer.stats_ == {"mean(B)": 0.0}
    assert scaler.stats_ == {
        "mean(A)": 0.0,
        "mean(B)": 1.0,
        "std(A)": 1.0,
        "std(B)": 0.0,
    }
    assert encoder.stats_ == {
        "unique_values(C)": {"monday": 0, "sunday": 1, "tuesday": 2}
    }

    expected_df = pd.DataFrame.from_dict(
        {"A": [-1.0, -1.0, 1.0, 1.0], "B": [0.0] * 4, "C": [1, 0, 2, 2]}
    ).astype(out_df.dtypes.to_dict())
    pd.testing.assert_frame_equal(out_df, expected_df, check_like=True)


def test_deferred_fit_matches_eager_fit(deferred_fit_enabled):
    ds = _example_dataset()

    ctx = ray.data.DataContext.get_current()
    ctx.enable_aggregation_based_preprocessors = False
    eager_chain, eager_members = _example_chain()
    eager_out = eager_chain.fit_transform(ds).to_pandas()

    ctx.enable_aggregation_based_preprocessors = True
    deferred_chain, deferred_members = _example_chain()
    deferred_out = deferred_chain.fit_transform(ds).to_pandas()

    for eager, deferred in zip(eager_members, deferred_members):
        assert eager.stats_ == deferred.stats_, type(eager).__name__
    pd.testing.assert_frame_equal(
        deferred_out.sort_index(axis=1), eager_out.sort_index(axis=1)
    )


def test_deferred_fit_batches_independent_members(
    deferred_fit_enabled, counted_aggregations
):
    """Members touching disjoint columns fit in ONE aggregation query."""
    ds = ray.data.from_pandas(
        pd.DataFrame({"A": [1.0, 2.0], "B": [3.0, 4.0], "C": ["x", "y"]})
    )
    chain = Chain(
        StandardScaler(["A"], output_columns=["A_scaled"]),
        StandardScaler(["B"], output_columns=["B_scaled"]),
        LabelEncoder("C"),
    )
    chain.fit(ds)
    chain.transform(ds).to_pandas()

    assert len(counted_aggregations) == 1, (
        f"independent members should share one aggregation query, "
        f"got {counted_aggregations}"
    )
    # 2 aggregations per scaler (mean, std) + 1 for the encoder.
    assert counted_aggregations[0] == 5


def test_deferred_fit_one_scan_per_dependency_level(
    deferred_fit_enabled, counted_aggregations
):
    ds = _example_dataset()
    chain, _ = _example_chain()
    chain.fit(ds)
    chain.transform(ds).to_pandas()

    # Level 0: scaler (mean/std of A and B) + encoder (unique of C) = 5 aggs.
    # Level 1: imputer's mean(B) over the scaled data = 1 agg.
    assert counted_aggregations == [5, 1]


def test_repeated_transform_does_not_recompute(
    deferred_fit_enabled, counted_aggregations
):
    ds = _example_dataset()
    chain, _ = _example_chain()
    chain.fit(ds)

    out1 = chain.transform(ds).to_pandas()
    queries_after_first = len(counted_aggregations)
    out2 = chain.transform(ds).to_pandas()
    out3 = chain.transform(ds).to_pandas()

    assert (
        len(counted_aggregations) == queries_after_first
    ), "repeated transform must not re-run aggregations"
    pd.testing.assert_frame_equal(out1, out2)
    pd.testing.assert_frame_equal(out2, out3)


def test_deferred_stats_computed_on_fit_dataset(deferred_fit_enabled):
    """Statistics come from the dataset passed to fit(), even when a different
    dataset is transformed first."""
    train = ray.data.from_pandas(pd.DataFrame({"A": [0.0, 2.0]}))
    val = ray.data.from_pandas(pd.DataFrame({"A": [100.0, 200.0]}))

    scaler = StandardScaler(["A"])
    chain = Chain(scaler)
    chain.fit(train)

    chain.transform(val).to_pandas()

    assert scaler.stats_ == {"mean(A)": 1.0, "std(A)": 1.0}


def _make_add_one(input_column: str, output_column: str) -> Preprocessor:
    """Non-fittable preprocessor writing a new column.

    Defined in a function so cloudpickle serializes the class by value; a
    module-level test class isn't importable from Ray workers.
    """

    class _AddOne(Preprocessor):
        _is_fittable = False

        def __init__(self):
            super().__init__()
            self._input_column = input_column
            self._output_column = output_column

        def get_input_columns(self):
            return [self._input_column]

        def get_output_columns(self):
            return [self._output_column]

        def _transform_pandas(self, df):
            df[self._output_column] = df[self._input_column] + 1
            return df

    return _AddOne()


def test_fittable_member_depending_on_non_fittable_output(deferred_fit_enabled):
    """A member reading a non-fittable member's output column must aggregate
    only after that member's transform has been applied."""
    ds = ray.data.from_pandas(pd.DataFrame({"A": [0.0, 2.0]}))

    add_one = _make_add_one("A", "D")  # D = A + 1 -> [1.0, 3.0]
    scaler = StandardScaler(["D"])
    chain = Chain(add_one, scaler)
    chain.fit(ds)

    out_df = chain.transform(ds).to_pandas()

    assert scaler.stats_ == {"mean(D)": 2.0, "std(D)": 1.0}
    assert list(out_df["D"]) == [-1.0, 1.0]


def test_unmarked_member_uses_sequential_fit(deferred_fit_enabled):
    """A chain containing a member that doesn't support deferred fitting falls
    back to the eager sequential path for the whole chain."""
    from ray.data.preprocessors import CountVectorizer

    ds = ray.data.from_pandas(
        pd.DataFrame({"A": [0.0, 2.0], "text": ["hello world", "hello ray"]})
    )

    scaler = StandardScaler(["A"])
    vectorizer = CountVectorizer(["text"])
    assert not vectorizer._supports_deferred_fit
    chain = Chain(scaler, vectorizer)
    chain.fit(ds)

    # Eager sequential path: stats exist right after fit.
    assert scaler.has_stats()
    assert vectorizer.has_stats()

    out_df = chain.transform(ds).to_pandas()
    assert list(out_df["A"]) == [-1.0, 1.0]


def _make_callable_stat_scaler(columns) -> StandardScaler:
    """A StandardScaler that claims deferred-fit support but registers a
    callable stat, to exercise the serial fallback safety valve at
    materialization time. Defined in a function so cloudpickle serializes the
    class by value (see `_make_add_one`)."""

    class _CallableStatScaler(StandardScaler):
        def _fit(self, dataset):
            from ray.data.aggregate import Mean, Std

            def compute(key_gen):
                stats = dataset.aggregate(
                    *[Mean(on=col) for col in self._columns],
                    *[Std(on=col, ddof=0) for col in self._columns],
                )
                return {key_gen(col): stats for col in self._columns}

            self._stat_computation_plan.add_callable_stat(
                stat_fn=compute,
                stat_key_fn=lambda col: f"all({col})",
                columns=self._columns,
            )
            return self

        def _fit_execute(self, dataset):
            super()._fit_execute(dataset)
            if self.stats_:
                merged = {}
                for col in self._columns:
                    merged.update(self.stats_.pop(f"all({col})"))
                self.stats_ = merged
            return self

    return _CallableStatScaler(columns)


def test_custom_stat_member_falls_back_to_serial(deferred_fit_enabled, caplog):
    ds = ray.data.from_pandas(pd.DataFrame({"A": [0.0, 2.0], "B": [1.0, 3.0]}))

    custom = _make_callable_stat_scaler(["A"])
    scaler = StandardScaler(["B"])
    chain = Chain(custom, scaler)
    chain.fit(ds)
    assert not scaler.has_stats(), "fit still defers"

    with caplog.at_level("WARNING", logger="ray.data.preprocessors.chain"):
        out_df = chain.transform(ds).to_pandas()

    assert any("Falling back to serial" in record.message for record in caplog.records)
    assert custom.stats_ == {"mean(A)": 1.0, "std(A)": 1.0}
    assert scaler.stats_ == {"mean(B)": 2.0, "std(B)": 1.0}
    assert list(out_df["A"]) == [-1.0, 1.0]
    assert list(out_df["B"]) == [-1.0, 1.0]


def test_same_aggregation_twice_in_one_level(deferred_fit_enabled):
    """Two members needing the same statistic on the same column can't share
    one query's result column; both must still fit correctly."""
    ds = ray.data.from_pandas(pd.DataFrame({"A": [0.0, 2.0]}))

    scaler1 = StandardScaler(["A"], output_columns=["A1"])
    scaler2 = StandardScaler(["A"], output_columns=["A2"])
    chain = Chain(scaler1, scaler2)
    chain.fit(ds)
    out_df = chain.transform(ds).to_pandas()

    assert scaler1.stats_ == {"mean(A)": 1.0, "std(A)": 1.0}
    assert scaler2.stats_ == {"mean(A)": 1.0, "std(A)": 1.0}
    assert list(out_df["A1"]) == [-1.0, 1.0]
    assert list(out_df["A2"]) == [-1.0, 1.0]


def test_nested_chain_deferred(deferred_fit_enabled):
    """A nested chain's members fit on the data flowing into the nested chain
    at its position in the outer chain."""
    ds = ray.data.from_pandas(pd.DataFrame({"B": [1.0, 3.0, None]}))

    imputer = SimpleImputer(["B"])  # mean(B) = 2.0
    inner = Chain(imputer)
    scaler = StandardScaler(["B"])  # over imputed [1, 3, 2]
    chain = Chain(inner, scaler)
    chain.fit(ds)
    assert not imputer.has_stats()

    chain.transform(ds).to_pandas()

    assert imputer.stats_ == {"mean(B)": 2.0}
    assert scaler.stats_ == {
        "mean(B)": 2.0,
        "std(B)": pytest.approx(0.8164965809),
    }


def test_serialization_materializes_deferred_stats(deferred_fit_enabled):
    from ray.data.preprocessor import SerializablePreprocessorBase

    ds = ray.data.from_pandas(pd.DataFrame({"A": [0.0, 2.0]}))

    scaler = StandardScaler(["A"])
    chain = Chain(scaler)
    chain.fit(ds)
    assert not scaler.has_stats()

    serialized = chain.serialize()
    # Serializing forced the deferred computation.
    assert scaler.stats_ == {"mean(A)": 1.0, "std(A)": 1.0}

    deserialized = SerializablePreprocessorBase.deserialize(serialized)
    result = deserialized.transform_batch(pd.DataFrame({"A": [1.0, 3.0]}))
    assert list(result["A"]) == [0.0, 2.0]


def test_transform_batch_materializes_deferred_stats(deferred_fit_enabled):
    ds = ray.data.from_pandas(pd.DataFrame({"A": [0.0, 2.0]}))

    scaler = StandardScaler(["A"])
    chain = Chain(scaler)
    chain.fit(ds)
    assert not scaler.has_stats()

    result = chain.transform_batch(pd.DataFrame({"A": [1.0, 3.0]}))
    assert scaler.stats_ == {"mean(A)": 1.0, "std(A)": 1.0}
    assert list(result["A"]) == [0.0, 2.0]


def test_flag_off_preserves_eager_fit():
    ctx = ray.data.DataContext.get_current()
    original = ctx.enable_aggregation_based_preprocessors
    ctx.enable_aggregation_based_preprocessors = False
    try:
        ds = _example_dataset()
        chain, (scaler, imputer, encoder) = _example_chain()
        chain.fit(ds)

        assert scaler.has_stats()
        assert imputer.has_stats()
        assert encoder.has_stats()
    finally:
        ctx.enable_aggregation_based_preprocessors = original


def test_all_scalers_defer_in_chain(deferred_fit_enabled, counted_aggregations):
    """Every scaler fits via the stat computation plan, so a chain of scalers
    on disjoint columns fits in one aggregation query."""
    from ray.data.preprocessors import MaxAbsScaler, MinMaxScaler, RobustScaler

    ds = ray.data.from_pandas(
        pd.DataFrame(
            {
                "A": [0.0, 2.0],
                "B": [1.0, 3.0],
                "C": [-4.0, 2.0],
                "D": [0.0, 10.0],
            }
        )
    )
    minmax = MinMaxScaler(["A"], output_columns=["A_s"])
    maxabs = MaxAbsScaler(["B"], output_columns=["B_s"])
    robust = RobustScaler(["C"], output_columns=["C_s"])
    standard = StandardScaler(["D"], output_columns=["D_s"])
    chain = Chain(minmax, maxabs, robust, standard)
    chain.fit(ds)

    assert not minmax.has_stats(), "scalers should defer inside a chain"
    assert not maxabs.has_stats()
    assert not robust.has_stats()

    out_df = chain.transform(ds).to_pandas()

    assert len(counted_aggregations) == 1, counted_aggregations
    # min/max + abs_max + 3 quantiles + mean/std = 8 aggregations, one query.
    assert counted_aggregations[0] == 8

    assert minmax.stats_ == {"min(A)": 0.0, "max(A)": 2.0}
    assert maxabs.stats_ == {"abs_max(B)": 3.0}
    assert robust.stats_["low_quantile(C)"] == -4.0
    assert robust.stats_["high_quantile(C)"] == 2.0
    # The sketch's median of two values is one of them.
    assert robust.stats_["median(C)"] in (-4.0, 2.0)
    assert list(out_df["A_s"]) == [0.0, 1.0]
    assert list(out_df["B_s"]) == [pytest.approx(1 / 3), 1.0]
    assert list(out_df["D_s"]) == [-1.0, 1.0]


def test_dag_structure(deferred_fit_enabled):
    from ray.data.preprocessors.dag import (
        _AggregationNode,
        _build_aggregation_dag,
    )

    ds = _example_dataset()
    chain, (scaler, imputer, encoder) = _example_chain()
    chain.fit(ds)

    nodes = _build_aggregation_dag([scaler, imputer, encoder])

    scaler_nodes = [n for n in nodes if n.preprocessor is scaler]
    imputer_nodes = [n for n in nodes if n.preprocessor is imputer]
    encoder_nodes = [n for n in nodes if n.preprocessor is encoder]

    assert len(scaler_nodes) == 4  # mean/std x A/B
    assert len(imputer_nodes) == 1  # mean(B)
    assert len(encoder_nodes) == 1  # unique_values(C)
    assert all(isinstance(n, _AggregationNode) for n in nodes)

    # Scaler writes B; imputer reads B -> the imputer node depends on every
    # scaler node (dependencies are per-preprocessor, not per-column).
    for node in imputer_nodes:
        assert {dep.preprocessor for dep in node.dependencies} == {scaler}
    # The encoder only touches C -> fully independent.
    for node in encoder_nodes:
        assert not node.dependencies
    for node in scaler_nodes:
        assert not node.dependencies


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
