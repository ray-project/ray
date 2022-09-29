import pytest
from ray._private.test_utils import run_string_as_driver_nonblocking


def test_basic_workflow_logs(workflow_start_regular):
    script = """
import ray
from ray import workflow

ray.init(address='auto')

@ray.remote(**workflow.options(task_id="f"))
def f():
    return 10

workflow.run(f.bind(), workflow_id="wid")
    """
    proc = run_string_as_driver_nonblocking(script)
    logs = proc.stdout.read().decode("ascii") + proc.stderr.read().decode("ascii")
    # on driver
    assert 'Workflow job created. [id="wid"' in logs
    # # in WorkflowManagementActor's run_or_resume.remote()
    # assert "run_or_resume: wid" in logs
    # assert "Workflow job [id=wid] started." in logs
    # in _workflow_task_executor_remote
    assert "Task status [RUNNING]\t[wid@f" in logs
    assert "Task status [SUCCESSFUL]\t[wid@f" in logs


def test_chained_workflow_logs(workflow_start_regular):
    script = """
import ray
from ray import workflow

ray.init(address='auto')

@ray.remote(**workflow.options(task_id="f1"))
def f1():
    return 10

@ray.remote(**workflow.options(task_id="f2"))
def f2(x):
    return x+1

workflow.run(f2.bind(f1.bind()), workflow_id="wid1")
    """
    proc = run_string_as_driver_nonblocking(script)
    logs = proc.stdout.read().decode("ascii") + proc.stderr.read().decode("ascii")
    # on driver
    assert 'Workflow job created. [id="wid1"' in logs
    # # in WorkflowManagementActor's run_or_resume.remote()
    # assert "run_or_resume: wid1" in logs
    # assert "Workflow job [id=wid1] started." in logs
    # in _workflow_task_executor_remote
    assert "Task status [RUNNING]\t[wid1@f1" in logs
    assert "Task status [SUCCESSFUL]\t[wid1@f1" in logs
    assert "Task status [RUNNING]\t[wid1@f2" in logs
    assert "Task status [SUCCESSFUL]\t[wid1@f2" in logs


def test_dynamic_workflow_logs(workflow_start_regular):
    script = """
import ray
from ray import workflow

ray.init(address='auto')

@ray.remote(**workflow.options(task_id="f3"))
def f3(x):
    return x+1

@ray.remote(**workflow.options(task_id="f4"))
def f4(x):
    return f3.bind(x*2)

workflow.run(f4.bind(10), workflow_id="wid2")
    """
    proc = run_string_as_driver_nonblocking(script)
    logs = proc.stdout.read().decode("ascii") + proc.stderr.read().decode("ascii")
    # on driver
    assert 'Workflow job created. [id="wid2"' in logs
    # # in WorkflowManagementActor's run_or_resume.remote()
    # assert "run_or_resume: wid2" in logs
    # assert "Workflow job [id=wid2] started." in logs
    # in _workflow_task_executor_remote
    assert "Task status [RUNNING]\t[wid2@f3" in logs
    assert "Task status [SUCCESSFUL]\t[wid2@f3" in logs
    assert "Task status [RUNNING]\t[wid2@f4" in logs
    assert "Task status [SUCCESSFUL]\t[wid2@f4" in logs


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
