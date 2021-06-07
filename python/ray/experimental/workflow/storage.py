import json
import pathlib
from typing import Dict, List, Optional, Any, Set

import ray
import ray.cloudpickle

from ray.experimental.workflow.common import Workflow
from ray.experimental.workflow import checkpoint_result
from ray.experimental.workflow import workflow_context

from ray.experimental.workflow.constants import (
    STEPS_DIR, TASK_BODY_FILE, OBJECTS_DIR, NON_BLOCKING_CHECKPOINTING,
    STEP_INPUTS_METADATA, STEP_OUTPUTS_METADATA)


def _get_current_store_dir():
    context = workflow_context.get_workflow_step_context()
    return context.workflow_root_dir / context.workflow_id


class WorkflowStepLogger:
    def __init__(self, step_id: Optional[str] = None):
        current_store_dir = _get_current_store_dir()
        all_steps_dir: pathlib.Path = current_store_dir / STEPS_DIR
        objects_dir = current_store_dir / OBJECTS_DIR
        if not objects_dir.exists():
            objects_dir.mkdir(parents=True, exist_ok=True)
        if step_id is None:
            scope = workflow_context.get_scope()
            if scope:
                step_dir = all_steps_dir / scope[-1]
            else:
                step_dir = all_steps_dir
        else:
            step_dir = all_steps_dir / step_id
        if not step_dir.exists():
            step_dir.mkdir(parents=True, exist_ok=False)
        self.objects_dir = objects_dir
        self.step_dir = step_dir

    def save_task_body(self, func):
        pickled_function = ray.cloudpickle.dumps(func)
        with open(self.step_dir / TASK_BODY_FILE, "wb") as f:
            f.write(pickled_function)

    def save_inputs_metadata(self, metadata: Dict[str, Any]):
        with open(self.step_dir / STEP_INPUTS_METADATA, "w") as f:
            json.dump(metadata, f, indent=4)

    def save_outputs_metadata(self, metadata: Dict[str, Any]):
        with open(self.step_dir / STEP_OUTPUTS_METADATA, "w") as f:
            json.dump(metadata, f, indent=4)

    def save_objects(self, object_refs: List[ray.ObjectRef]):
        checkpoint_result.checkpoint_refs(
            object_refs,
            self.objects_dir,
            nonblocking=NON_BLOCKING_CHECKPOINTING)


def _save_workflow_inputs(workflow: Workflow):
    step_logger = WorkflowStepLogger(workflow.id)
    step_logger.save_objects([workflow._input_placeholder])
    f = workflow._original_function
    step_logger.save_task_body(f)
    step_logger.save_inputs_metadata(workflow.get_metadata())


def save_workflow_dag(workflow: Workflow):
    assert not workflow.executed
    workflows: Set[Workflow] = set()
    workflow._visit_workflow_dag(workflows)
    for w in workflows:
        _save_workflow_inputs(w)
    step_logger = WorkflowStepLogger()
    step_logger.save_outputs_metadata({
        "output_type": "workflow",
        "step_id": workflow.id,
    })


def save_workflow_output(output: Any):
    if not isinstance(output, ray.ObjectRef):
        rref = ray.put(output)
    else:
        rref = output
    step_logger = WorkflowStepLogger()
    step_logger.save_objects([rref])
    step_logger.save_outputs_metadata({
        "output_type": "object",
        "object_id": rref.hex(),
    })
