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


def test_custom_stat_member_falls_back_to_serial(
    deferred_fit_enabled, propagate_logs, caplog
):
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


def test_fit_levels():
    from ray.data.preprocessors.dag import _fit_levels

    scaler = StandardScaler(["A", "B"])
    imputer = SimpleImputer(["B"])
    encoder = LabelEncoder("C")

    # The imputer reads B, which the scaler writes -> next level. The encoder
    # only touches C -> shares the scaler's scan.
    assert _fit_levels([scaler, imputer, encoder]) == [0, 1, 0]

    # A member that doesn't declare both its input and output columns may
    # read and write any column: it waits for everything earlier, and
    # everything later waits for it.
    undeclared = _make_undeclared_in_place_scale("B", 10)
    assert _fit_levels([scaler, undeclared, encoder]) == [0, 1, 2]

    # Transforms apply strictly in chain order, so depending on a member also
    # means waiting for every member before it: d depends only on c, but c's
    # transform can't be applied until b (level 1) has fitted, pushing d to
    # level 2 - not the pure-DAG level 1.
    a, b = StandardScaler(["x"]), SimpleImputer(["x"])
    c, d = StandardScaler(["y"]), SimpleImputer(["y"])
    assert _fit_levels([a, b, c, d]) == [0, 1, 0, 2]


def test_fit_order():
    from ray.data.preprocessors import Concatenator
    from ray.data.preprocessors.dag import _fit_order

    scaler_x, imputer_x = StandardScaler(["x"]), SimpleImputer(["x"])
    scaler_y, imputer_y = StandardScaler(["y"]), SimpleImputer(["y"])

    # The (x, y) pairs are independent, so the scalers regroup to share the
    # first scan and the imputers the second.
    assert _fit_order([scaler_x, imputer_x, scaler_y, imputer_y]) == [0, 2, 1, 3]

    # Anti-dependency: the concatenator overwrites x, which the imputer
    # reads. It has no read-after-write dependency on anything (its own
    # input is y), but moving it earlier would rewrite x before the imputer
    # fits on it, so it must keep its position.
    concatenator = Concatenator(columns=["y"], output_column_name="x")
    assert _fit_order([scaler_x, imputer_x, concatenator]) == [0, 1, 2]

    # Destructive read: Concatenator drops the columns it reads, so even a
    # read-read overlap pins the order. The wide scaler sits at conflict
    # level 1 (it reads x, which scaler_x writes); if the concatenator could
    # jump to level 0, it would drop y before the wide scaler aggregates
    # over it.
    wide_scaler = StandardScaler(["x", "y"], output_columns=["x_s", "y_s"])
    dropping = Concatenator(columns=["y"], output_column_name="f")
    assert _fit_order([scaler_x, wide_scaler, dropping]) == [0, 1, 2]

    # A member without declared columns conflicts with everything, pinning
    # the order on both sides.
    undeclared = _make_undeclared_in_place_scale("x", 10)
    assert _fit_order([scaler_x, undeclared, scaler_y]) == [0, 1, 2]


def test_fit_reordering_reduces_scans(deferred_fit_enabled, monkeypatch):
    import ray.data.preprocessors.chain as chain_module

    real_execute = chain_module.execute_aggregate_specs
    scans = []

    def counting_execute(ds, specs):
        scans.append(sorted(spec.stat_fn.name for spec in specs))
        return real_execute(ds, specs)

    monkeypatch.setattr(chain_module, "execute_aggregate_specs", counting_execute)

    ds = ray.data.from_pandas(
        pd.DataFrame({"x": [1.0, 2.0, None, 4.0], "y": [10.0, None, 30.0, 40.0]})
    )
    scaler_x, imputer_x = StandardScaler(["x"]), SimpleImputer(["x"])
    scaler_y, imputer_y = StandardScaler(["y"]), SimpleImputer(["y"])
    chain = Chain(scaler_x, imputer_x, scaler_y, imputer_y)

    out = chain.fit_transform(ds).to_pandas()

    # In chain order this fit needs 3 scans (levels [0, 1, 0, 2]); fitted in
    # `_fit_order`'s permutation both scalers share scan 0 and both imputers
    # share scan 1.
    assert scans == [
        ["mean(x)", "mean(y)", "std(x)", "std(y)"],
        ["mean(x)", "mean(y)"],
    ]
    # Statistics match sequential fitting: the imputers fit on standardized
    # columns, whose mean is 0, and the transformed output has no nulls left.
    assert imputer_x.stats_["mean(x)"] == pytest.approx(0.0)
    assert imputer_y.stats_["mean(y)"] == pytest.approx(0.0)
    assert not out["x"].isna().any() and not out["y"].isna().any()


def _make_undeclared_in_place_scale(
    column: str, factor: float, declare_inputs: bool = False
) -> Preprocessor:
    """Non-fittable preprocessor that rewrites a column in place without
    declaring the columns it writes (the default for user-defined
    preprocessors). With `declare_inputs`, it sets `columns` like many
    user-defined preprocessors do, so only its input columns are declared.
    Defined in a function for the same reason as `_make_add_one`.
    """

    class _UndeclaredScale(Preprocessor):
        _is_fittable = False

        def __init__(self):
            super().__init__()
            if declare_inputs:
                self.columns = [column]

        def _transform_pandas(self, df):
            df[column] = df[column] * factor
            return df

    return _UndeclaredScale()


@pytest.mark.parametrize("declare_inputs", [False, True])
@pytest.mark.parametrize("nested", [False, True])
def test_member_without_declared_columns_is_a_barrier(
    deferred_fit_enabled, nested, declare_inputs
):
    """A member that doesn't declare both its input and output columns may
    write any column, so later members must not compute statistics until its
    transform has been applied."""
    ds = ray.data.from_pandas(pd.DataFrame({"a": [1.0, 2.0, 3.0]}))
    custom = _make_undeclared_in_place_scale("a", 10, declare_inputs=declare_inputs)
    scaler = StandardScaler(["a"])
    chain = Chain(Chain(custom) if nested else custom, scaler)

    out = chain.fit_transform(ds).to_pandas()

    # Same statistics as fitting step by step: computed on a * 10.
    assert scaler.stats_["mean(a)"] == pytest.approx(20.0)
    assert list(out["a"]) == pytest.approx([-1.2247449, 0.0, 1.2247449])


def test_member_without_declared_columns_keeps_its_fit_position(
    deferred_fit_enabled,
):
    """Fit-time reordering must not move members across a member that doesn't
    declare its columns: it may rewrite any column, so moving it ahead of a
    member that still has to fit would change that member's statistics."""
    from ray.data.preprocessors.dag import _fit_order

    ds = ray.data.from_pandas(
        pd.DataFrame({"x": [1.0, 2.0, 3.0], "y": [10.0, 20.0, 30.0]})
    )
    scaler_x = StandardScaler(["x"])
    rescaler_x = StandardScaler(["x"])  # fits on scaler_x's output
    undeclared = _make_undeclared_in_place_scale("x", 10)
    scaler_y = StandardScaler(["y"])
    members = [scaler_x, rescaler_x, undeclared, scaler_y]

    assert _fit_order(members) == [0, 1, 2, 3]

    Chain(*members).fit_transform(ds).to_pandas()

    # Same as fitting step by step: rescaler_x sees standardized x (std 1),
    # not standardized x * 10.
    assert rescaler_x.stats_["std(x)"] == pytest.approx(1.0)


def test_member_without_base_init(deferred_fit_enabled):
    """A non-fittable user-defined member that doesn't call
    `Preprocessor.__init__` (and so has no stat computation plan) works in a
    deferred chain, as it does with eager fitting."""

    class _NoBaseInit(Preprocessor):
        _is_fittable = False

        def __init__(self):
            # NOTE: Intentionally doesn't call `super().__init__()`.
            pass

        def _transform_pandas(self, df):
            df["a"] = df["a"] * 10
            return df

    ds = ray.data.from_pandas(pd.DataFrame({"a": [1.0, 2.0, 3.0]}))
    scaler = StandardScaler(["a"])

    Chain(_NoBaseInit(), scaler).fit_transform(ds).to_pandas()

    assert scaler.stats_["mean(a)"] == pytest.approx(20.0)


def test_later_member_does_not_rewrite_data_before_earlier_member_fits(
    deferred_fit_enabled,
):
    """A later member that overwrites a column an earlier member reads must not
    transform the data before the earlier member has computed its statistics,
    even when the two don't depend on each other's outputs."""
    ds = ray.data.from_pandas(
        pd.DataFrame({"a": [1.0, None, 3.0], "b": [100.0, 200.0, 300.0]})
    )
    imputer = SimpleImputer(["a"])
    scaler = StandardScaler(["a"])  # waits for the imputer (reads a)
    overwrite_a = _make_add_one("b", "a")  # independent, but rewrites a

    Chain(imputer, scaler, overwrite_a).fit_transform(ds).to_pandas()

    # The scaler sees the imputed `a` = [1, 2, 3], not `b + 1`.
    assert scaler.stats_["mean(a)"] == pytest.approx(2.0)


def test_member_waits_for_the_transform_it_depends_on(deferred_fit_enabled):
    """Transforms are applied in chain order, so a member whose input comes
    from an earlier member must wait until that member's transform has actually
    been applied, not just until it has finished computing statistics."""
    ds = ray.data.from_pandas(
        pd.DataFrame({"a": [1.0, None, 3.0], "b": [100.0, 200.0, 300.0]})
    )
    imputer = SimpleImputer(["a"])
    scaler_a = StandardScaler(["a"])  # waits for the imputer
    add_one = _make_add_one("b", "c")  # independent, but queued behind scaler_a
    scaler_c = StandardScaler(["c"])  # needs add_one's output

    Chain(imputer, scaler_a, add_one, scaler_c).fit_transform(ds).to_pandas()

    assert scaler_a.stats_["mean(a)"] == pytest.approx(2.0)
    assert scaler_c.stats_["mean(c)"] == pytest.approx(201.0)


@pytest.mark.parametrize("serial_fallback", [False, True])
def test_categorizer_with_some_dtypes_given(deferred_fit_enabled, serial_fallback):
    """`Categorizer` fills `stats_` with the dtypes it was given before
    registering aggregations for the other columns; those must still run."""
    from ray.data.preprocessors import Categorizer

    ds = ray.data.from_pandas(
        pd.DataFrame({"A": ["x", "y", "x"], "B": ["p", "q", "r"], "n": [0.0, 1.0, 2.0]})
    )
    categorizer = Categorizer(["A", "B"], dtypes={"A": pd.CategoricalDtype(["x", "y"])})
    members = [categorizer]
    if serial_fallback:
        # A member with a callable stat makes the chain fit step by step.
        members.insert(0, _make_callable_stat_scaler(["n"]))
    chain = Chain(*members)

    out = chain.fit_transform(ds).to_pandas()

    assert list(categorizer.stats_["B"].categories) == ["p", "q", "r"]
    assert isinstance(out["B"].dtype, pd.CategoricalDtype)


@pytest.mark.parametrize("serial_fallback", [False, True])
def test_fit_dataset_released_after_stats_are_computed(
    deferred_fit_enabled, serial_fallback
):
    """The chain only needs the fit dataset to compute the deferred statistics,
    so it must not keep the dataset alive afterwards."""
    ds = ray.data.from_pandas(pd.DataFrame({"a": [0.0, 2.0], "n": [1.0, 3.0]}))
    members = [StandardScaler(["a"])]
    if serial_fallback:
        members.append(_make_callable_stat_scaler(["n"]))
    chain = Chain(*members)
    chain.fit(ds)
    assert chain._fit_ds is not None

    first = chain.transform(ds).to_pandas()

    assert chain._fit_ds is None
    # Later transforms and serialization still work without it.
    pd.testing.assert_frame_equal(chain.transform(ds).to_pandas(), first)
    assert Chain.deserialize(chain.serialize()).transform_batch(
        pd.DataFrame({"a": [1.0], "n": [2.0]})
    )["a"].tolist() == [0.0]


def test_chain_input_and_output_columns():
    """A Chain's inputs exclude columns an earlier member produces, and both
    lists are de-duplicated in order."""
    chain = Chain(
        SimpleImputer(["a"], output_columns=["a_imputed"]),
        StandardScaler(["a_imputed"]),  # reads a column produced in the chain
        StandardScaler(["a"]),
    )
    assert chain.get_input_columns() == ["a"]
    assert chain.get_output_columns() == ["a_imputed", "a"]

    in_place = Chain(SimpleImputer(["a"]), StandardScaler(["a"]))
    assert in_place.get_input_columns() == ["a"]
    assert in_place.get_output_columns() == ["a"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
