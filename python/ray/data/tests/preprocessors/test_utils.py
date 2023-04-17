import pytest

from ray.data.preprocessors.utils import simple_hash, simple_split_tokenizer


def test_simple_split_tokenizer():
    # Tests simple_split_tokenizer.
    assert simple_split_tokenizer("one_word") == ["one_word"]
    assert simple_split_tokenizer("two words") == ["two", "words"]
    assert simple_split_tokenizer("One fish. Two fish.") == [
        "One",
        "fish.",
        "Two",
        "fish.",
    ]


def test_simple_hash():
    # Tests simple_hash determinism.
    assert simple_hash(1, 100) == 83
    assert simple_hash("a", 100) == 52
    assert simple_hash("banana", 100) == 16
    assert simple_hash([1, 2, "apple"], 100) == 37


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
