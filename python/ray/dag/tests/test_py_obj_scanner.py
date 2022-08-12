from ray.dag.py_obj_scanner import _PyObjScanner, _instances
import pytest
import gc


class Source:
    pass


def test_simple_replace():
    scanner = _PyObjScanner(source_type=Source)
    my_objs = [Source(), [Source(), {"key": Source()}]]

    found = scanner.find_nodes(my_objs)
    assert len(found) == 3

    replaced = scanner.replace_nodes({obj: 1 for obj in found})
    assert replaced == [1, [1, {"key": 1}]]


class NotSerializable:
    def __reduce__(self):
        raise Exception("don't even try to serialize me.")


def test_not_serializing_objects():
    scanner = _PyObjScanner(source_type=Source)
    not_serializable = NotSerializable()
    my_objs = [not_serializable, {"key": Source()}]

    found = scanner.find_nodes(my_objs)
    assert len(found) == 1

    replaced = scanner.replace_nodes({obj: 1 for obj in found})
    assert replaced == [not_serializable, {"key": 1}]


def test_scanner_gc():
    """Test gc collect after the delete instances[id] called"""
    prev_len = len(_instances)

    def call_find_nodes():
        scanner = _PyObjScanner(source_type=Source)
        my_objs = [Source(), [Source(), {"key": Source()}]]
        scanner.find_nodes(my_objs)
        gc.collect()
        assert len(gc.get_referrers(scanner)) == 1
        del _instances[id(scanner)]
        gc.collect()
        assert len(gc.get_referrers(scanner)) == 0

    call_find_nodes()
    assert prev_len == len(_instances)

    def call_find_and_replace_nodes():
        scanner = _PyObjScanner(source_type=Source)
        my_objs = [Source(), [Source(), {"key": Source()}]]
        found = scanner.find_nodes(my_objs)
        replaced = scanner.replace_nodes({obj: 1 for obj in found})
        gc.collect()
        assert len(gc.get_referrers(scanner)) == 1
        del _instances[id(scanner)]
        gc.collect()
        assert len(gc.get_referrers(scanner)) == 0

    call_find_and_replace_nodes()
    assert prev_len == len(_instances)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
