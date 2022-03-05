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
    "results_preprocessor",
    [
        AverageResultsPreprocessor,
        MaxResultsPreprocessor,
    ],
)
def test_metrics_warning_in_aggregate_results_preprocessors(
    caplog, results_preprocessor
):
    import logging
    from ray.util import debug

    caplog.at_level(logging.WARNING)

    results1 = [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}]
    results2 = [{"a": 1}, {"a": "invalid"}, {"a": 3}, {"a": "invalid"}]

    results_preprocessor1 = results_preprocessor(["b"])
    results_preprocessor1.preprocess(results1)

    results_preprocessor2 = results_preprocessor(["a"])
    results_preprocessor2.preprocess(results2)

    for record in caplog.records:
        assert record.levelname == "WARNING"

    assert "`b` is not reported from workers, so it is ignored." in caplog.text
    assert "`a` value type is not valid, so it is ignored." in caplog.text

    debug.reset_log_once("b")
    debug.reset_log_once("a")


def test_warning_in_weighted_average_results_preprocessors(caplog):
    import logging

    caplog.at_level(logging.WARNING)

    results1 = [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}]
    results2 = [{"b": 1}, {"b": 2}, {"b": 3}, {"b": 4}]
    results3 = [
        {"a": 1, "c": 1},
        {"a": 2, "c": "invalid"},
        {"a": 3, "c": 2},
        {"a": 4, "c": "invalid"},
    ]

    results_preprocessor1 = WeightedAverageResultsPreprocessor(["a"], "b")
    results_preprocessor1.preprocess(results1)
    results_preprocessor1.preprocess(results2)

    results_preprocessor2 = WeightedAverageResultsPreprocessor(["a"], "c")
    results_preprocessor2.preprocess(results3)

    for record in caplog.records:
        assert record.levelname == "WARNING"

    assert (
        "Averaging weight `b` is not reported by all workers in `train.report()`."
        in caplog.text
    )
    assert "`a` is not reported from workers, so it is ignored." in caplog.text
    assert "Averaging weight `c` value type is not valid." in caplog.text
    assert "Use equal weight instead." in caplog.text


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
