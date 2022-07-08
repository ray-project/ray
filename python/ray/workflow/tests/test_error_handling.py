import pytest

import ray
from ray import workflow


def test_step_failure(workflow_start_regular_shared, tmp_path):
    @workflow.options(max_retries=10)
    @ray.remote
    def unstable_task_exception(n):
        v = int((tmp_path / "test").read_text())
        (tmp_path / "test").write_text(f"{v + 1}")
        if v < n:
            raise ValueError("Invalid")
        return v

    @workflow.options(max_retries=10)
    @ray.remote
    def unstable_task_crash(n):
        v = int((tmp_path / "test").read_text())
        (tmp_path / "test").write_text(f"{v + 1}")
        if v < n:
            import os

            os.kill(os.getpid(), 9)
        return v

    @workflow.options(max_retries=10)
    @ray.remote
    def unstable_task_crash_then_exception(n):
        v = int((tmp_path / "test").read_text())
        (tmp_path / "test").write_text(f"{v + 1}")
        if v < n / 2:
            import os

            os.kill(os.getpid(), 9)
        elif v < n:
            raise ValueError("Invalid")
        return v

    with pytest.raises(Exception):
        workflow.run_async(
            unstable_task_exception.options(**workflow.options(max_retries=-2).bind())
        )

    for task in (
        unstable_task_exception,
        unstable_task_crash,
        unstable_task_crash_then_exception,
    ):
        (tmp_path / "test").write_text("0")
        assert workflow.run(task.bind(10)) == 10

        (tmp_path / "test").write_text("0")
        with pytest.raises(workflow.WorkflowExecutionError):
            workflow.run(task.bind(11))

    # TODO(suquark): catch crash as an exception
    for task in (unstable_task_exception, unstable_task_crash_then_exception):
        (tmp_path / "test").write_text("0")
        (ret, err) = workflow.run(
            task.options(**workflow.options(catch_exceptions=True)).bind(10)
        )
        assert ret == 10
        assert err is None

        (tmp_path / "test").write_text("0")
        (ret, err) = workflow.run(
            task.options(**workflow.options(catch_exceptions=True)).bind(11)
        )
        assert ret is None
        assert err is not None


def test_nested_catch_exception(workflow_start_regular_shared):
    @ray.remote
    def f2():
        return 10

    @ray.remote
    def f1():
        return workflow.continuation(f2.bind())

    assert (10, None) == workflow.run(
        f1.options(**workflow.options(catch_exceptions=True)).bind()
    )


def test_nested_catch_exception_2(workflow_start_regular_shared):
    @ray.remote
    def f1(n):
        if n == 0:
            raise ValueError()
        else:
            return workflow.continuation(f1.bind(n - 1))

    ret, err = workflow.run(
        f1.options(**workflow.options(catch_exceptions=True)).bind(5)
    )
    assert ret is None
    assert isinstance(err, ValueError)


def test_nested_catch_exception_3(workflow_start_regular_shared, tmp_path):
    """Test the case where the exception is not raised by the output task of
    a nested DAG."""

    @ray.remote
    def f3():
        return 10

    @ray.remote
    def f3_exc():
        raise ValueError()

    @ray.remote
    def f2(x):
        return x

    @ray.remote
    def f1(exc):
        if exc:
            return workflow.continuation(f2.bind(f3_exc.bind()))
        else:
            return workflow.continuation(f2.bind(f3.bind()))

    ret, err = workflow.run(
        f1.options(**workflow.options(catch_exceptions=True)).bind(True)
    )
    assert ret is None
    assert isinstance(err, ValueError)

    assert (10, None) == workflow.run(
        f1.options(**workflow.options(catch_exceptions=True)).bind(False)
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
