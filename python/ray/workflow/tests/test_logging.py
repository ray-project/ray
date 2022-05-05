from ray._private.test_utils import run_string_as_driver_nonblocking


def test_basic_workflow_logs(workflow_start_regular):
    script = """
import ray
from ray import workflow

ray.init(address='auto')

@workflow.step(name="f")
def f():
    return 10

f.step().run("wid")
    """
    proc = run_string_as_driver_nonblocking(script)
    logs = proc.stdout.read().decode("ascii") + proc.stderr.read().decode("ascii")
    # on driver
    assert 'Workflow job created. [id="wid"' in logs
    # # in WorkflowManagementActor's run_or_resume.remote()
    # assert "run_or_resume: wid" in logs
    # assert "Workflow job [id=wid] started." in logs
    # in _workflow_step_executor_remote
    assert "Step status [RUNNING]\t[wid@f" in logs
    assert "Step status [SUCCESSFUL]\t[wid@f" in logs


def test_chained_workflow_logs(workflow_start_regular):
    script = """
import ray
from ray import workflow

ray.init(address='auto')

@workflow.step(name="f1")
def f1():
    return 10

@workflow.step(name="f2")
def f2(x):
    return x+1

f2.step(f1.step()).run("wid1")
    """
    proc = run_string_as_driver_nonblocking(script)
    logs = proc.stdout.read().decode("ascii") + proc.stderr.read().decode("ascii")
    # on driver
    assert 'Workflow job created. [id="wid1"' in logs
    # # in WorkflowManagementActor's run_or_resume.remote()
    # assert "run_or_resume: wid1" in logs
    # assert "Workflow job [id=wid1] started." in logs
    # in _workflow_step_executor_remote
    assert "Step status [RUNNING]\t[wid1@f1" in logs
    assert "Step status [SUCCESSFUL]\t[wid1@f1" in logs
    assert "Step status [RUNNING]\t[wid1@f2" in logs
    assert "Step status [SUCCESSFUL]\t[wid1@f2" in logs


def test_dynamic_workflow_logs(workflow_start_regular):
    script = """
import ray
from ray import workflow

ray.init(address='auto')

@workflow.step(name="f3")
def f3(x):
    return x+1

@workflow.step(name="f4")
def f4(x):
    return f3.step(x*2)

f4.step(10).run("wid2")
    """
    proc = run_string_as_driver_nonblocking(script)
    logs = proc.stdout.read().decode("ascii") + proc.stderr.read().decode("ascii")
    # on driver
    assert 'Workflow job created. [id="wid2"' in logs
    # # in WorkflowManagementActor's run_or_resume.remote()
    # assert "run_or_resume: wid2" in logs
    # assert "Workflow job [id=wid2] started." in logs
    # in _workflow_step_executor_remote
    assert "Step status [RUNNING]\t[wid2@f3" in logs
    assert "Step status [SUCCESSFUL]\t[wid2@f3" in logs
    assert "Step status [RUNNING]\t[wid2@f4" in logs
    assert "Step status [SUCCESSFUL]\t[wid2@f4" in logs


def test_virtual_actor_logs(workflow_start_regular):
    script = """
import ray
from ray import workflow

ray.init(address='auto')

@workflow.virtual_actor
class Counter:
    def __init__(self, x: int):
        self.x = x

    def add(self, y):
        self.x += y
        return self.x

couter = Counter.get_or_create("vid", 10)
couter.add.options(name="add").run(1)
    """
    proc = run_string_as_driver_nonblocking(script)
    logs = proc.stdout.read().decode("ascii") + proc.stderr.read().decode("ascii")
    print(logs)
    # on driver
    assert 'Workflow job created. [id="vid"' in logs
    # # in WorkflowManagementActor's run_or_resume.remote()
    # assert "run_or_resume: vid" in logs
    # assert "Workflow job [id=vid] started." in logs
    # in _workflow_step_executor_remote
    assert "Step status [RUNNING]\t[vid@add" in logs
    assert "Step status [SUCCESSFUL]\t[vid@add" in logs
