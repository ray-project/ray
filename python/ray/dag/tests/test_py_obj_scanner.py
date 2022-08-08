from ray.dag.py_obj_scanner import _PyObjScanner


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
