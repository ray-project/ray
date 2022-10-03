from concurrent.futures import ThreadPoolExecutor

from ray.dag.py_obj_scanner import PyObjScanner
import pytest


class Source:
    pass


def test_simple_replace():
    with PyObjScanner(
        [Source(), [Source(), {"key": Source()}]], source_type=Source
    ) as scanner:
        assert scanner.found_objects == 3
        scanner.replace_with_dict({obj: 1 for obj in scanner.found_objects})
        assert scanner.reconstruct() == [1, [1, {"key": 1}]]

        scanner.replace_with_list([2, 2, 2])
        assert scanner.reconstruct() == [2, [2, {"key": 2}]]

        scanner.replace_with_func(lambda x: 3)
        assert scanner.reconstruct() == [3, [3, {"key": 3}]]

    from ray.dag.py_obj_scanner import _local

    # test the resources are GCed correctly
    assert not _local.stack


class NotSerializable:
    def __reduce__(self):
        raise Exception("don't even try to serialize me.")


def test_not_serializing_objects():
    not_serializable = NotSerializable()
    with PyObjScanner(
        [not_serializable, {"key": Source()}], source_type=Source
    ) as scanner:
        assert len(scanner.found_objects) == 1

        scanner.replace_with_dict({obj: 1 for obj in scanner.found_objects})
        assert scanner.reconstruct() == [not_serializable, {"key": 1}]


def test_scanner_multi_thread():
    inputs = [Source() for _ in range(100000)]

    def _worker(x):
        with PyObjScanner(x, source_type=Source) as scanner:
            scanner.replace_with_func(lambda n: 1)
            assert sum(scanner.reconstruct()) == 100000

    with ThreadPoolExecutor(max_workers=100) as pool:
        for _ in range(10):
            list(pool.map(_worker, [inputs] * 100))


def test_nested():
    with PyObjScanner(
        [Source(), [Source(), {"key": Source()}]], source_type=Source
    ) as scanner:
        with PyObjScanner(
            [Source(), {"key": Source()}], source_type=Source
        ) as scanner_2:
            assert scanner_2.found_objects == 2
            scanner_2.replace_with_dict({obj: 1 for obj in scanner.found_objects})
            assert scanner_2.reconstruct() == [1, {"key": 1}]
        assert scanner.found_objects == 3
        scanner.replace_with_dict({obj: 1 for obj in scanner.found_objects})
        assert scanner.reconstruct() == [1, [1, {"key": 1}]]

    from ray.dag.py_obj_scanner import _local

    # test the resources are GCed correctly
    assert not _local.stack


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
