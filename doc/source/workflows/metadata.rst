Workflow Metadata
=================

Observability is important for workflows - sometimes we not only want
to get the output, but also want to gain insights on the internal
states (e.g., to measure the performance or find bottlenecks).
Workflow metadata provides several stats that help understand
the workflow, from basic running status and task options to performance
and user-imposed metadata.

Retrieving metadata
-------------------
Workflow metadata can be retrieved with ``workflow.get_metadata(workflow_id)``.
For example:

.. code-block:: python

    @ray.remote
    def add(left: int, right: int) -> int:
        return left + right

    workflow.create(add.bind(10, 20)).run("add_example")

    workflow_metadata = workflow.get_metadata("add_example")

    assert workflow_metadata["status"] == "SUCCESSFUL"
    assert "start_time" in workflow_metadata["stats"]
    assert "end_time" in workflow_metadata["stats"]

You can also retrieve metadata for individual workflow tasks by
providing the task name:

.. code-block:: python

    workflow.create(add.options(**workflow.options(name="add_task")).bind(10, 20)).run("add_example_2")

    task_metadata = workflow.get_metadata("add_example_2", name="add_task")

    assert "task_options" in task_metadata
    assert "start_time" in workflow_metadata["stats"]
    assert "end_time" in workflow_metadata["stats"]

User-defined metadata
---------------------
Custom metadata can be added to a workflow or a workflow task by the user,
which is useful when you want to attach some extra information to the
workflow or workflow task.

- workflow-level metadata can be added via ``.run(metadata=metadata)``
- task-level metadata can be added via ``.options(**workflow.options(metadata=metadata))`` or in the decorator ``@workflow.options(metadata=metadata)``

.. code-block:: python

    workflow.create(add.options(**workflow.options(name="add_task", metadata={"task_k": "task_v"})).bind(10, 20))\
    .run("add_example_3", metadata={"workflow_k": "workflow_v"})

    assert workflow.get_metadata("add_example_3")["user_metadata"] == {"workflow_k": "workflow_v"}
    assert workflow.get_metadata("add_example_3", name="add_task")["user_metadata"] == {"task_k": "task_v"}

**Note: user-defined metadata must be a python dictionary with values that are
JSON serializable.**

Metadata in Virtual Actors
--------------------------
Virtual Actors also support metadata ingestion and retrieval. For example:

.. code-block:: python

    @workflow.virtual_actor
    class Actor:
        def __init__(self, v):
            self.v = v

        def add_v(self, v):
            time.sleep(1)
            self.v += v
            return self.v

    actor = Actor.get_or_create("vid", 0)

    actor.add_v.options(name="add", metadata={"k1": "v1"}).run(10)
    actor.add_v.options(name="add", metadata={"k2": "v2"}).run(10)
    actor.add_v.options(name="add", metadata={"k3": "v3"}).run(10)

    assert workflow.get_metadata("vid", "add")["user_metadata"] == {"k1": "v1"}
    assert workflow.get_metadata("vid", "add_1")["user_metadata"] == {"k2": "v2"}
    assert workflow.get_metadata("vid", "add_2")["user_metadata"] == {"k3": "v3"}
    assert workflow.get_metadata("vid", "add")["stats"]["end_time"] >= workflow.get_metadata("vid", "add")["stats"]["start_time"] + 1
    assert workflow.get_metadata("vid", "add_1")["stats"]["end_time"] >= workflow.get_metadata("vid", "add_1")["stats"]["start_time"] + 1
    assert workflow.get_metadata("vid", "add_2")["stats"]["end_time"] >= workflow.get_metadata("vid", "add_2")["stats"]["start_time"] + 1

Notice that if there are multiple tasks with the same name, a suffix
with a counter _n will be added automatically.

And you can also do this in a nested way:

.. code-block:: python

    @workflow.virtual_actor
    class Counter:
        def __init__(self):
            self.n = 0

        def incr(self, n):
            self.n += 1
            if n - 1 > 0:
                return self.incr.options(
                    name="incr", metadata={
                        "current_n": self.n
                    }).step(n - 1)
            else:
                return self.n

    counter = Counter.get_or_create("counter")
    counter.incr.options(name="incr", metadata={"outer_k": "outer_v"}).run(5)

    assert workflow.get_metadata("counter", "incr")["user_metadata"] == {"outer_k": "outer_v"}
    assert workflow.get_metadata("counter", "incr_1")["user_metadata"] == {"current_n": 1}
    assert workflow.get_metadata("counter", "incr_2")["user_metadata"] == {"current_n": 2}
    assert workflow.get_metadata("counter", "incr_3")["user_metadata"] == {"current_n": 3}
    assert workflow.get_metadata("counter", "incr_4")["user_metadata"] == {"current_n": 4}

Available Metrics
-----------------
**Workflow level**

- status: workflow states, can be one of RUNNING, FAILED, RESUMABLE, CANCELED, or SUCCESSFUL.
- user_metadata: a python dictionary of custom metadata by the user via ``workflow.run()``.
- stats: workflow running stats, including workflow start time and end time.

**Task level**

- name: name of the task, either provided by the user via ``task.options(**workflow.options(name=xxx))`` or generated by the system.
- task_options: options of the task, either provided by the user via ``task.options()`` or default by system.
- user_metadata: a python dictionary of custom metadata by the user via ``task.options()``.
- stats: task running stats, including task start time and end time.


Notes
-----
1. Unlike ``get_output()``, ``get_metadata()`` returns an immediate
result for the time it is called, this also means not all fields will
be available in the result if corresponding metadata is not available
(e.g., ``metadata["stats"]["end_time"]`` won't be available until the workflow
is completed).

.. code-block:: python

    @ray.remote
    def simple():
        flag.touch() # touch a file here
        time.sleep(1000)
        return 0

    workflow.create(simple.bind()).run_async(workflow_id)

    # make sure workflow task starts running
    while not flag.exists():
        time.sleep(1)

    workflow_metadata = workflow.get_metadata(workflow_id)
    assert workflow_metadata["status"] == "RUNNING"
    assert "start_time" in workflow_metadata["stats"]
    assert "end_time" not in workflow_metadata["stats"]

    workflow.cancel(workflow_id)

    workflow_metadata = workflow.get_metadata(workflow_id)
    assert workflow_metadata["status"] == "CANCELED"
    assert "start_time" in workflow_metadata["stats"]
    assert "end_time" not in workflow_metadata["stats"]

2. For resumed workflows, the current behavior is that "stats" will
be updated whenever a workflow is resumed.

.. code-block:: python

    workflow_id = "simple"
    error_flag = tmp_path / "error"
    error_flag.touch()

    @ray.remote
    def simple():
        if error_flag.exists():
            raise ValueError()
        return 0

    with pytest.raises(ray.exceptions.RaySystemError):
        workflow.create(simple.bind()).run(workflow_id)

    workflow_metadata_failed = workflow.get_metadata(workflow_id)
    assert workflow_metadata_failed["status"] == "FAILED"

    # remove flag to make task success
    error_flag.unlink()
    ref = workflow.resume(workflow_id)
    assert ray.get(ref) == 0

    workflow_metadata_resumed = workflow.get_metadata(workflow_id)
    assert workflow_metadata_resumed["status"] == "SUCCESSFUL"

    # make sure resume updated running metrics
    assert  workflow_metadata_resumed["stats"]["start_time"] > workflow_metadata_failed["stats"]["start_time"]
    assert workflow_metadata_resumed["stats"]["end_time"] > workflow_metadata_failed["stats"]["end_time"]

