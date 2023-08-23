import tempfile
import time
import warnings

import pytest

import ray
from ray.air._internal.util import StartTraceback
from ray.train._internal.accelerator import Accelerator
from ray.train._internal.storage import StorageContext
from ray.air.constants import SESSION_MISUSE_LOG_ONCE_KEY
from ray.train._internal.session import (
    init_session,
    shutdown_session,
    get_session,
    get_accelerator,
    set_accelerator,
)
from ray.air.session import (
    get_checkpoint,
    get_world_rank,
    get_local_rank,
    report,
    get_dataset_shard,
    get_world_size,
)
from ray.train.error import SessionMisuseError

from ray.train.tests.util import create_dict_checkpoint, load_dict_checkpoint


storage = StorageContext(
    storage_path=tempfile.mkdtemp(),
    experiment_dir_name="exp_name",
    trial_dir_name="trial_name",
)


@pytest.fixture(scope="function")
def session():
    def f():
        return 1

    init_session(
        training_func=f,
        world_rank=0,
        local_rank=0,
        node_rank=0,
        local_world_size=1,
        world_size=1,
        storage=storage,
    )
    yield get_session()
    shutdown_session()


@pytest.fixture(autouse=True)
def shutdown():
    if get_session():
        shutdown_session()


def test_init_fail(session):
    with pytest.raises(ValueError):
        init_session(lambda: 1, 0)


def test_shutdown(session):
    shutdown_session()
    assert not get_session()


def test_world_rank(session):
    assert get_world_rank() == 0
    shutdown_session()
    # Make sure default to 0.
    assert get_world_rank() == 0


def test_local_rank(session):
    assert get_local_rank() == 0
    shutdown_session()
    # Make sure default to 0.
    assert get_local_rank() == 0


def test_world_size(session):
    assert get_world_size() == 1
    shutdown_session()
    # Make sure default to 1.
    assert get_world_size() == 1


def test_train(session):
    session.start()
    output = session.finish()
    assert output == 1


def test_get_dataset_shard(shutdown_only):
    dataset = ray.data.from_items([1, 2, 3])
    init_session(
        training_func=lambda: 1,
        world_rank=0,
        local_rank=0,
        node_rank=0,
        local_world_size=1,
        world_size=1,
        dataset_shard=dataset,
        storage=storage,
    )
    assert get_dataset_shard() == dataset
    shutdown_session()


def test_report():
    def train_func():
        for i in range(2):
            report(dict(loss=i))

    init_session(
        training_func=train_func,
        world_rank=0,
        local_rank=0,
        node_rank=0,
        local_world_size=1,
        world_size=1,
        storage=storage,
    )
    session = get_session()
    session.start()
    assert session.get_next().metrics["loss"] == 0
    assert session.get_next().metrics["loss"] == 1
    shutdown_session()


def test_report_fail():
    def train_func():
        for i in range(2):
            report(i)
        return 1

    init_session(
        training_func=train_func,
        world_rank=0,
        local_rank=0,
        node_rank=0,
        local_world_size=1,
        world_size=1,
        storage=storage,
    )
    session = get_session()
    session.start()
    with pytest.raises(StartTraceback):
        session.get_next()
    shutdown_session()


def test_report_after_finish(session):
    session.start()
    session.pause_reporting()
    session.finish()
    for _ in range(2):
        report(dict(loss=1))
    assert session.get_next() is None
    shutdown_session()


def test_no_start(session):
    with pytest.raises(RuntimeError):
        session.get_next()
    shutdown_session()


def test_checkpoint():
    def train_func():
        for i in range(2):
            with create_dict_checkpoint(dict(epoch=i)) as checkpoint:
                report({}, checkpoint=checkpoint)

    def validate_zero(expected):
        next = session.get_next()
        assert next is not None and next.checkpoint is not None
        assert load_dict_checkpoint(next.checkpoint)["epoch"] == expected

    init_session(
        training_func=train_func,
        world_rank=0,
        local_rank=0,
        node_rank=0,
        local_world_size=1,
        world_size=1,
        storage=storage,
    )
    session = get_session()
    session.start()
    validate_zero(0)
    validate_zero(1)
    session.finish()
    shutdown_session()


def test_load_checkpoint_after_save():
    def train_func():
        for i in range(2):
            with create_dict_checkpoint(dict(epoch=i)) as checkpoint:
                report(dict(epoch=i), checkpoint=checkpoint)
            checkpoint = get_checkpoint()
            assert load_dict_checkpoint(checkpoint)["epoch"] == i

    init_session(
        training_func=train_func,
        world_rank=0,
        local_rank=0,
        node_rank=0,
        local_world_size=1,
        world_size=1,
        storage=storage,
    )
    session = get_session()
    session.start()
    for i in range(2):
        session.get_next()
    session.finish()
    shutdown_session()


def test_locking():
    """Tests that report pauses training until fetch_next or finish."""

    def train_1():
        import _thread

        _thread.interrupt_main()

    init_session(
        training_func=train_1,
        world_rank=0,
        local_rank=0,
        node_rank=0,
        local_world_size=1,
        world_size=1,
        storage=storage,
    )
    session = get_session()
    with pytest.raises(KeyboardInterrupt):
        session.start()
    shutdown_session()

    def train_2():
        for i in range(2):
            report(dict(loss=i))
        train_1()

    init_session(
        training_func=train_2,
        world_rank=0,
        local_rank=0,
        node_rank=0,
        local_world_size=1,
        world_size=1,
        storage=storage,
    )
    session = get_session()
    session.start()
    time.sleep(3)

    session.pause_reporting()
    # Releases session.continue_lock to resume the training thread.
    session.get_next()

    with pytest.raises(KeyboardInterrupt):
        session.get_next()
    session.finish()
    shutdown_session()


def reset_log_once_with_str(str_to_append=None):
    key = SESSION_MISUSE_LOG_ONCE_KEY
    if str_to_append:
        key += f"-{str_to_append}"
    ray.util.debug.reset_log_once(key)


@pytest.mark.parametrize("fn", [get_checkpoint, get_dataset_shard])
def test_warn(fn):
    """Checks if calling session functions outside of session raises warning."""

    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        # Ignore Deprecation warnings.
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        assert not fn()

    assert fn.__name__ in record[0].message.args[0]

    reset_log_once_with_str(fn.__name__)


def test_warn_report():
    """Checks if calling session.report function outside of session raises warning."""

    fn = report

    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        # Ignore Deprecation warnings.
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        assert not fn(dict())

    assert fn.__name__ in record[0].message.args[0]

    reset_log_once_with_str(fn.__name__)


def test_warn_once():
    """Checks if session misuse warning is only shown once per function."""

    with warnings.catch_warnings(record=True) as record:
        # Ignore Deprecation warnings.
        warnings.simplefilter("always")
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        assert not get_checkpoint()
        assert not get_checkpoint()
        assert not report(dict(x=2))
        assert not report(dict(x=2))
        assert not get_dataset_shard()
        assert not get_dataset_shard()

    # Should only warn once.
    assert len(record) == 3


class FakeAccelerator(Accelerator):
    pass


def test_set_and_get_accelerator(session):
    accelerator = FakeAccelerator()
    set_accelerator(accelerator)
    assert get_accelerator(FakeAccelerator) is accelerator


def test_get_accelerator_constructs_default_accelerator(session):
    assert isinstance(get_accelerator(FakeAccelerator), FakeAccelerator)


def test_get_accelerator_raises_error_outside_session():
    with pytest.raises(SessionMisuseError):
        get_accelerator(FakeAccelerator)


def test_set_accelerator_raises_error_if_accelerator_already_set(session):
    accelerator1, accelerator2 = FakeAccelerator(), FakeAccelerator()
    set_accelerator(accelerator1)
    with pytest.raises(RuntimeError):
        set_accelerator(accelerator2)


def test_set_accelerator_raises_error_outside_session():
    accelerator = FakeAccelerator()
    with pytest.raises(SessionMisuseError):
        set_accelerator(accelerator)


def test_application_error_raised():
    def f():
        raise ValueError

    init_session(
        training_func=f,
        world_rank=0,
        local_rank=0,
        node_rank=0,
        local_world_size=1,
        world_size=1,
        storage=storage,
    )
    session = get_session()
    session.start()
    with pytest.raises(StartTraceback):
        session.get_next()
    shutdown_session()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
