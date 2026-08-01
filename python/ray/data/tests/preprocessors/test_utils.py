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
    assert simple_hash(1, 100) == 15
    assert simple_hash("a", 100) == 99
    assert simple_hash("banana", 100) == 10
    assert simple_hash([1, 2, "apple"], 100) == 58


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
