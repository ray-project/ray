from typing import Any

import pytest

from ray.dag.py_obj_scanner import _instances, _PyObjScanner


class Source:
    pass


def test_simple_replace():
    scanner = _PyObjScanner(source_type=Source)
    my_objs = [Source(), [Source(), {"key": Source()}]]

    found = scanner.find_nodes(my_objs)
    assert len(found) == 3

    replaced = scanner.replace_nodes({obj: 1 for obj in found})
    assert replaced == [1, [1, {"key": 1}]]


def test_replace_multiple_types():
    class OtherSource:
        pass

    scanner = _PyObjScanner(source_type=(Source, OtherSource))
    my_objs = [Source(), [Source(), {"key": Source(), "key2": OtherSource()}]]

    found = scanner.find_nodes(my_objs)
    assert len(found) == 4

    replaced = scanner.replace_nodes(
        {obj: 1 if isinstance(obj, Source) else 2 for obj in found}
    )
    assert replaced == [1, [1, {"key": 1, "key2": 2}]]


def test_replace_nested_in_obj():
    """Test that the source can be nested in arbitrary objects."""
    scanner = _PyObjScanner(source_type=Source)

    class Outer:
        def __init__(self, inner: Any):
            self._inner = inner

        def __eq__(self, other):
            return self._inner == other._inner

    my_objs = [Outer(Source()), Outer(Outer(Source())), Outer((Source(),))]

    found = scanner.find_nodes(my_objs)
    assert len(found) == 3

    replaced = scanner.replace_nodes({obj: 1 for obj in found})
    assert replaced == [Outer(1), Outer(Outer(1)), Outer((1,))]


def test_scanner_clear():
    """Test scanner clear to make the scanner GCable"""
    prev_len = len(_instances)

    def call_find_nodes():
        scanner = _PyObjScanner(source_type=Source)
        my_objs = [Source(), [Source(), {"key": Source()}]]
        scanner.find_nodes(my_objs)
        scanner.clear()
        assert id(scanner) not in _instances

    call_find_nodes()
    assert prev_len == len(_instances)

    def call_find_and_replace_nodes():
        scanner = _PyObjScanner(source_type=Source)
        my_objs = [Source(), [Source(), {"key": Source()}]]
        found = scanner.find_nodes(my_objs)
        scanner.replace_nodes({obj: 1 for obj in found})
        scanner.clear()
        assert id(scanner) not in _instances

    call_find_and_replace_nodes()
    assert prev_len == len(_instances)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
