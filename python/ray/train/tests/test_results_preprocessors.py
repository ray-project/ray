from ray.train.callbacks.results_preprocessors import \
    ExcludedKeysResultsPreprocessor, IndexedResultsPreprocessor, \
    SequentialResultsPreprocessor


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
    results = [{
        "a": 1,
        "b": 2
    }, {
        "a": 3,
        "b": 4
    }, {
        "a": 5,
        "b": 6
    }, {
        "a": 7,
        "b": 8
    }]
    expected = [{"b": 2}, {"b": 6}]

    preprocessor_1 = ExcludedKeysResultsPreprocessor("a")
    # [{"b": 2}, {"b": 4}, {"b": 6}, {"b": 8}]
    preprocessor_2 = IndexedResultsPreprocessor([0, 2])

    preprocessor = SequentialResultsPreprocessor(
        [preprocessor_1, preprocessor_2])
    preprocessed_results = preprocessor.preprocess(results)

    assert preprocessed_results == expected


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
