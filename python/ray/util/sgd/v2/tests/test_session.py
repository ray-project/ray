import time
import pytest

from ray.util.sgd.v2.session import init_session, shutdown_session, \
    get_session, world_rank, report


@pytest.fixture(scope="function")
def session():
    def f():
        return 1

    init_session(training_func=f, world_rank=0)
    yield get_session()
    shutdown_session()


def test_init_fail(session):
    with pytest.raises(ValueError):
        init_session(lambda: 1, 0)


def test_get_fail(session):
    shutdown_session()
    with pytest.raises(ValueError):
        get_session()


def test_world_rank(session):
    assert world_rank() == 0
    shutdown_session()
    with pytest.raises(ValueError):
        world_rank()


def test_train(session):
    session.start()
    output = session.finish()
    assert output == 1


def test_report():
    def train():
        for i in range(2):
            report(loss=i)

    init_session(training_func=train, world_rank=0)
    session = get_session()
    session.start()
    assert session.get_next()["loss"] == 0
    assert session.get_next()["loss"] == 1
    shutdown_session()

    with pytest.raises(ValueError):
        report(loss=2)


def test_report_fail():
    def train():
        for i in range(2):
            report(i)
        return 1

    init_session(training_func=train, world_rank=0)
    session = get_session()
    session.start()
    assert session.get_next() is None
    with pytest.raises(TypeError):
        session.finish()
    shutdown_session()


def test_report_after_finish(session):
    session.start()
    session.finish()
    for _ in range(2):
        report(loss=1)
    assert session.get_next() is None


def test_no_start(session):
    with pytest.raises(RuntimeError):
        session.get_next()


def test_locking():
    """Tests that report pauses training until fetch_next or finish."""

    def train_1():
        import _thread
        _thread.interrupt_main()

    init_session(training_func=train_1, world_rank=0)
    session = get_session()
    with pytest.raises(KeyboardInterrupt):
        session.start()
    shutdown_session()

    def train_2():
        for i in range(2):
            report(loss=i)
        train_1()

    init_session(training_func=train_2, world_rank=0)
    session = get_session()
    session.start()
    time.sleep(3)

    with pytest.raises(KeyboardInterrupt):
        session.finish()
    shutdown_session()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
