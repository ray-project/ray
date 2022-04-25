import pytest

from ray.train.callbacks.results_preprocessors import (
    ExcludedKeysResultsPreprocessor,
    IndexedResultsPreprocessor,
    SequentialResultsPreprocessor,
    AverageResultsPreprocessor,
    MaxResultsPreprocessor,
    WeightedAverageResultsPreprocessor,
)


def test_excluded_keys_results_preprocessor():
    results = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    expected = [{"b": 2}, {"b": 4}]

    preprocessor = ExcludedKeysResultsPreprocessor("a")
    preprocessed_results = preprocessor.preprocess(results)

    assert preprocessed_results == expected


def test_indexed_results_preprocessor():
    results = [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}]
    expected = [{"a": 1}, {"a": 3}]

    preprocessor = IndexedResultsPreprocessor([0, 2])
    preprocessed_results = preprocessor.preprocess(results)

    assert preprocessed_results == expected


def test_sequential_results_preprocessor():
    results = [{"a": 1, "b": 2}, {"a": 3, "b": 4}, {"a": 5, "b": 6}, {"a": 7, "b": 8}]
    expected = [{"b": 2}, {"b": 6}]

    preprocessor_1 = ExcludedKeysResultsPreprocessor("a")
    # [{"b": 2}, {"b": 4}, {"b": 6}, {"b": 8}]
    preprocessor_2 = IndexedResultsPreprocessor([0, 2])

    preprocessor = SequentialResultsPreprocessor([preprocessor_1, preprocessor_2])
    preprocessed_results = preprocessor.preprocess(results)

    assert preprocessed_results == expected


def test_average_results_preprocessor():
    from copy import deepcopy
    import numpy as np

    results = [{"a": 1, "b": 2}, {"a": 3, "b": 4}, {"a": 5, "b": 6}, {"a": 7, "b": 8}]
    expected = deepcopy(results)
    for res in expected:
        res.update(
            {
                "avg(a)": np.mean([result["a"] for result in results]),
                "avg(b)": np.mean([result["b"] for result in results]),
            }
        )

    preprocessor = AverageResultsPreprocessor(["a", "b"])
    preprocessed_results = preprocessor.preprocess(results)

    assert preprocessed_results == expected


def test_max_results_preprocessor():
    from copy import deepcopy
    import numpy as np

    results = [{"a": 1, "b": 2}, {"a": 3, "b": 4}, {"a": 5, "b": 6}, {"a": 7, "b": 8}]
    expected = deepcopy(results)
    for res in expected:
        res.update(
            {
                "max(a)": np.max([result["a"] for result in results]),
                "max(b)": np.max([result["b"] for result in results]),
            }
        )

    preprocessor = MaxResultsPreprocessor(["a", "b"])
    preprocessed_results = preprocessor.preprocess(results)

    assert preprocessed_results == expected


def test_weighted_average_results_preprocessor():
    from copy import deepcopy
    import numpy as np

    results = [{"a": 1, "b": 2}, {"a": 3, "b": 4}, {"a": 5, "b": 6}, {"a": 7, "b": 8}]
    expected = deepcopy(results)
    total_weight = np.sum([result["b"] for result in results])
    for res in expected:
        res.update(
            {
                "weight_avg_b(a)": np.sum(
                    [result["a"] * result["b"] / total_weight for result in results]
                )
            }
        )

    preprocessor = WeightedAverageResultsPreprocessor(["a"], "b")
    preprocessed_results = preprocessor.preprocess(results)

    assert preprocessed_results == expected


@pytest.mark.parametrize(
    ("results_preprocessor", "expected_value"),
    [(AverageResultsPreprocessor, 2.0), (MaxResultsPreprocessor, 3.0)],
)
def test_warning_in_aggregate_results_preprocessors(
    caplog, results_preprocessor, expected_value
):
    import logging
    from copy import deepcopy
    from ray.util import debug

    caplog.at_level(logging.WARNING)

    results1 = [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}]
    results2 = [{"a": 1}, {"a": "invalid"}, {"a": 3}, {"a": "invalid"}]
    results3 = [{"a": "invalid"}, {"a": "invalid"}, {"a": "invalid"}, {"a": "invalid"}]
    results4 = [{"a": 1}, {"a": 2}, {"a": 3}, {"c": 4}]

    # test case 1: metric key `b` is missing from all workers
    results_preprocessor1 = results_preprocessor(["b"])
    results_preprocessor1.preprocess(results1)
    assert "`b` is not reported from workers, so it is ignored." in caplog.text

    # test case 2: some values of key `a` have invalid data type
    results_preprocessor2 = results_preprocessor(["a"])
    expected2 = deepcopy(results2)
    aggregation_key = results_preprocessor2.aggregate_fn.wrap_key("a")
    for res in expected2:
        res.update({aggregation_key: expected_value})
    assert results_preprocessor2.preprocess(results2) == expected2

    # test case 3: all key `a` values are invalid
    results_preprocessor2.preprocess(results3)
    assert "`a` value type is not valid, so it is ignored." in caplog.text

    # test case 4: some workers don't report key `a`
    expected4 = deepcopy(results4)
    aggregation_key = results_preprocessor2.aggregate_fn.wrap_key("a")
    for res in expected4:
        res.update({aggregation_key: expected_value})
    assert results_preprocessor2.preprocess(results4) == expected4

    for record in caplog.records:
        assert record.levelname == "WARNING"

    debug.reset_log_once("b")
    debug.reset_log_once("a")


def test_warning_in_weighted_average_results_preprocessors(caplog):
    import logging
    from copy import deepcopy

    caplog.at_level(logging.WARNING)

    results1 = [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}]
    results2 = [{"b": 1}, {"b": 2}, {"b": 3}, {"b": 4}]
    results3 = [
        {"a": 1, "c": 3},
        {"a": 2, "c": "invalid"},
        {"a": "invalid", "c": 1},
        {"a": 4, "c": "invalid"},
    ]
    results4 = [
        {"a": 1, "c": "invalid"},
        {"a": 2, "c": "invalid"},
        {"a": 3, "c": "invalid"},
        {"a": 4, "c": "invalid"},
    ]

    # test case 1: weight key `b` is not reported from all workers
    results_preprocessor1 = WeightedAverageResultsPreprocessor(["a"], "b")
    expected1 = deepcopy(results1)
    for res in expected1:
        res.update({"weight_avg_b(a)": 2.5})
    assert results_preprocessor1.preprocess(results1) == expected1
    assert (
        "Averaging weight `b` is not reported by all workers in `train.report()`."
        in caplog.text
    )
    assert "Use equal weight instead." in caplog.text

    # test case 2: metric key `a` (to be averaged) is not reported from all workers
    results_preprocessor1.preprocess(results2)
    assert "`a` is not reported from workers, so it is ignored." in caplog.text

    # test case 3: both metric and weight keys have invalid data type
    results_preprocessor2 = WeightedAverageResultsPreprocessor(["a"], "c")
    expected3 = deepcopy(results3)
    for res in expected3:
        res.update({"weight_avg_c(a)": 1.0})
    assert results_preprocessor2.preprocess(results3) == expected3

    # test case 4: all weight values are invalid
    expected4 = deepcopy(results4)
    for res in expected4:
        res.update({"weight_avg_c(a)": 2.5})
    assert results_preprocessor2.preprocess(results4) == expected4
    assert "Averaging weight `c` value type is not valid." in caplog.text

    for record in caplog.records:
        assert record.levelname == "WARNING"


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
