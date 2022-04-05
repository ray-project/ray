import time

import pytest

import ray
from ray.train.accelerator import Accelerator
from ray.train.constants import SESSION_MISUSE_LOG_ONCE_KEY
from ray.train.session import (
    init_session,
    shutdown_session,
    get_session,
    world_rank,
    local_rank,
    report,
    save_checkpoint,
    TrainingResultType,
    load_checkpoint,
    get_dataset_shard,
    world_size,
    get_accelerator,
    set_accelerator,
    SessionMisuseError,
)


@pytest.fixture(scope="function")
def session():
    def f():
        return 1

    init_session(training_func=f, world_rank=0, local_rank=0, world_size=1)
    yield get_session()
    shutdown_session()


def test_init_fail(session):
    with pytest.raises(ValueError):
        init_session(lambda: 1, 0)


def test_shutdown(session):
    shutdown_session()
    assert not get_session()


def test_world_rank(session):
    assert world_rank() == 0
    shutdown_session()
    # Make sure default to 0.
    assert world_rank() == 0


def test_local_rank(session):
    assert local_rank() == 0
    shutdown_session()
    # Make sure default to 0.
    assert local_rank() == 0


def test_world_size(session):
    assert world_size() == 1
    shutdown_session()
    # Make sure default to 1.
    assert world_size() == 1


def test_train(session):
    session.start()
    output = session.finish()
    assert output == 1


def test_get_dataset_shard():
    dataset = ray.data.from_items([1, 2, 3])
    init_session(
        training_func=lambda: 1,
        world_rank=0,
        local_rank=0,
        world_size=1,
        dataset_shard=dataset,
    )
    assert get_dataset_shard() == dataset
    shutdown_session()


def test_report():
    def train_func():
        for i in range(2):
            report(loss=i)

    init_session(training_func=train_func, world_rank=0, local_rank=0, world_size=1)
    session = get_session()
    session.start()
    assert session.get_next().data["loss"] == 0
    assert session.get_next().data["loss"] == 1
    shutdown_session()


def test_report_fail():
    def train_func():
        for i in range(2):
            report(i)
        return 1

    init_session(training_func=train_func, world_rank=0, local_rank=0, world_size=1)
    session = get_session()
    session.start()
    assert session.get_next() is None
    with pytest.raises(TypeError):
        session.finish()
    shutdown_session()


def test_report_after_finish(session):
    session.start()
    session.pause_reporting()
    session.finish()
    for _ in range(2):
        report(loss=1)
    assert session.get_next() is None


def test_no_start(session):
    with pytest.raises(RuntimeError):
        session.get_next()


def test_checkpoint():
    def train_func():
        for i in range(2):
            save_checkpoint(epoch=i)

    def validate_zero(expected):
        next = session.get_next()
        assert next is not None
        assert next.type == TrainingResultType.CHECKPOINT
        assert next.data["epoch"] == expected

    init_session(training_func=train_func, world_rank=0, local_rank=0, world_size=1)
    session = get_session()
    session.start()
    validate_zero(0)
    validate_zero(1)
    session.finish()
    shutdown_session()

    def validate_nonzero():
        next = session.get_next()
        assert next is not None
        assert next.type == TrainingResultType.CHECKPOINT
        assert next.data == {}

    init_session(training_func=train_func, world_rank=1, local_rank=1, world_size=1)
    session = get_session()
    session.start()
    validate_nonzero()
    validate_nonzero()
    session.finish()
    shutdown_session()


def test_encode_data():
    def train_func():
        save_checkpoint(epoch=0)
        report(epoch=1)

    def encode_checkpoint(checkpoint):
        checkpoint.update({"encoded": True})
        return checkpoint

    def validate_encoded(result_type: TrainingResultType):
        next = session.get_next()
        assert next.type is result_type
        assert next.data["encoded"] is True

    init_session(
        training_func=train_func,
        world_rank=0,
        local_rank=0,
        world_size=1,
        encode_data_fn=encode_checkpoint,
    )

    session = get_session()
    session.start()
    # Validate checkpoint is encoded.
    validate_encoded(TrainingResultType.CHECKPOINT)
    # Validate report is encoded.
    validate_encoded(TrainingResultType.REPORT)
    session.finish()
    shutdown_session()


def test_load_checkpoint_after_save():
    def train_func():
        for i in range(2):
            save_checkpoint(epoch=i)
            checkpoint = load_checkpoint()
            assert checkpoint["epoch"] == i

    init_session(training_func=train_func, world_rank=0, local_rank=0, world_size=1)
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

    init_session(training_func=train_1, world_rank=0, local_rank=0, world_size=1)
    session = get_session()
    with pytest.raises(KeyboardInterrupt):
        session.start()
    shutdown_session()

    def train_2():
        for i in range(2):
            report(loss=i)
        train_1()

    init_session(training_func=train_2, world_rank=0, local_rank=0, world_size=1)
    session = get_session()
    session.start()
    time.sleep(3)

    session.pause_reporting()
    # Releases session.continue_lock to resume the training thread.
    session.get_next()

    with pytest.raises(KeyboardInterrupt):
        session.finish()
    shutdown_session()


def reset_log_once_with_str(str_to_append=None):
    key = SESSION_MISUSE_LOG_ONCE_KEY
    if str_to_append:
        key += f"-{str_to_append}"
    ray.util.debug.reset_log_once(key)


@pytest.mark.parametrize(
    "fn", [load_checkpoint, save_checkpoint, report, get_dataset_shard]
)
def test_warn(fn):
    """Checks if calling train functions outside of session raises warning."""

    with pytest.warns(UserWarning) as record:
        fn()

    assert fn.__name__ in record[0].message.args[0]

    reset_log_once_with_str(fn.__name__)


def test_warn_once():
    """Checks if session misuse warning is only shown once per function."""

    with pytest.warns(UserWarning) as record:
        assert not load_checkpoint()
        assert not load_checkpoint()
        assert not save_checkpoint(x=2)
        assert not report(x=2)
        assert not report(x=3)
        assert not get_dataset_shard()

    # Should only warn once.
    assert len(record) == 4


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


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
