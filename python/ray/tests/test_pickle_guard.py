import io
import multiprocessing
import os
import pickle
import sys
import threading

import numpy as np
import pytest

import ray
import ray.cloudpickle as cloudpickle
from ray.util.pickle_guard import (
    UntrustedUnpicklingError,
    allow_unsafe_unpickling,
    forbid_untrusted_unpickling,
    guard_iterator,
    is_unpickling_forbidden,
)


class _Gadget:
    def __reduce__(self):
        return (os.system, ("echo should-never-run",))


class _Plain:
    def __init__(self, value):
        self.value = value


PRIMITIVE_PICKLE = pickle.dumps({"a": [1, 2.0, "s", b"b", None]})
CLASS_PICKLE = pickle.dumps(_Plain(1))
GADGET_PICKLE = pickle.dumps(_Gadget())


def test_primitive_pickle_loads_inside_forbid():
    with forbid_untrusted_unpickling():
        assert pickle.loads(PRIMITIVE_PICKLE) == {"a": [1, 2.0, "s", b"b", None]}


def test_class_pickle_blocked_inside_forbid():
    with forbid_untrusted_unpickling("Opt in with allow_pickle=True."), pytest.raises(
        UntrustedUnpicklingError, match="allow_pickle=True"
    ) as exc_info:
        pickle.loads(CLASS_PICKLE)
    assert "_Plain" in str(exc_info.value)


def test_deserialization_blocked_before_it_runs(tmp_path):
    marker = tmp_path / "marker"

    class Exploit:
        def __reduce__(self):
            return (os.system, (f"touch {marker}",))

    payload = pickle.dumps(Exploit())
    with forbid_untrusted_unpickling(), pytest.raises(UntrustedUnpicklingError):
        pickle.loads(payload)
    assert not marker.exists()


def test_python_unpickler_subclass_blocked():
    class MyUnpickler(pickle.Unpickler):
        pass

    with forbid_untrusted_unpickling(), pytest.raises(UntrustedUnpicklingError):
        MyUnpickler(io.BytesIO(CLASS_PICKLE)).load()


def test_numpy_object_array_blocked_inside_forbid_allowed_outside():
    payload = pickle.dumps(np.array(["a", "b"], dtype=object))
    with forbid_untrusted_unpickling(), pytest.raises(UntrustedUnpicklingError):
        pickle.loads(payload)
    assert pickle.loads(payload).tolist() == ["a", "b"]


def test_error_is_a_value_error():
    assert issubclass(UntrustedUnpicklingError, ValueError)


def test_allow_nested_inside_forbid_and_restores():
    with forbid_untrusted_unpickling():
        assert is_unpickling_forbidden()
        with allow_unsafe_unpickling():
            assert not is_unpickling_forbidden()
            assert pickle.loads(CLASS_PICKLE).value == 1
        assert is_unpickling_forbidden()
        with pytest.raises(UntrustedUnpicklingError):
            pickle.loads(CLASS_PICKLE)
    assert not is_unpickling_forbidden()


def test_ray_cloudpickle_is_exempt():
    blob = cloudpickle.dumps(_Plain(2))
    with forbid_untrusted_unpickling():
        assert cloudpickle.loads(blob).value == 2
        assert cloudpickle.load(io.BytesIO(blob)).value == 2
        with pytest.raises(UntrustedUnpicklingError):
            pickle.loads(blob)


def test_ray_cloudpickle_loads_accepts_keyword_arguments():
    buffers = []
    blob = cloudpickle.dumps(
        pickle.PickleBuffer(b"x" * 8), protocol=5, buffer_callback=buffers.append
    )
    with forbid_untrusted_unpickling():
        assert bytes(cloudpickle.loads(blob, buffers=buffers)) == b"x" * 8


def test_guard_iterator_scopes_only_the_producer():
    def producer():
        assert is_unpickling_forbidden()
        with pytest.raises(UntrustedUnpicklingError):
            pickle.loads(CLASS_PICKLE)
        yield 1
        assert is_unpickling_forbidden()
        yield 2

    seen = []
    for item in guard_iterator(producer):
        # The consumer runs between yields and must not be blocked.
        assert not is_unpickling_forbidden()
        assert pickle.loads(CLASS_PICKLE).value == 1
        seen.append(item)
    assert seen == [1, 2]
    assert not is_unpickling_forbidden()


def test_guard_iterator_covers_eager_iterables():
    def eager():
        # Everything happens while building the list, before iteration.
        with pytest.raises(UntrustedUnpicklingError):
            pickle.loads(CLASS_PICKLE)
        return [1]

    assert list(guard_iterator(eager)) == [1]


def test_guard_iterator_propagates_producer_error():
    def failing():
        yield 1
        raise RuntimeError("boom")

    it = guard_iterator(failing)
    assert next(it) == 1
    with pytest.raises(RuntimeError, match="boom"):
        next(it)


def test_helper_thread_is_not_covered():
    result = {}

    def unpickle_in_thread():
        try:
            result["value"] = pickle.loads(CLASS_PICKLE).value
        except UntrustedUnpicklingError:
            result["value"] = "blocked"

    with forbid_untrusted_unpickling():
        t = threading.Thread(target=unpickle_in_thread)
        t.start()
        t.join()
    assert result["value"] == 1


def _child_unpickles(blob):
    return pickle.loads(blob).value


@pytest.mark.skipif(sys.platform == "win32", reason="fork is POSIX only")
def test_forked_child_starts_unguarded():
    ctx = multiprocessing.get_context("fork")
    with forbid_untrusted_unpickling():
        with ctx.Pool(1) as pool:
            # The child inherits the flag through fork and must reset it, or it
            # would refuse to unpickle its own task.
            assert pool.apply(_child_unpickles, (CLASS_PICKLE,)) == 1


@pytest.mark.parametrize(
    "api",
    [
        forbid_untrusted_unpickling,
        allow_unsafe_unpickling,
        is_unpickling_forbidden,
        guard_iterator,
        UntrustedUnpicklingError,
    ],
)
def test_public_surface_is_annotated(api):
    assert api._annotated == api.__name__


def test_ray_get_inside_forbid(ray_start_regular_shared):
    # A numpy array needs numpy's reconstructor global to unpickle, which the
    # guard would refuse for external bytes; Ray's own transport is exempt.
    @ray.remote
    def make():
        return np.arange(3)

    with forbid_untrusted_unpickling():
        assert ray.get(make.remote()).sum() == 3
        with pytest.raises(UntrustedUnpicklingError):
            pickle.loads(pickle.dumps(np.arange(3)))


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
