Workflow Metadata
=================

Each workflow is linked with a collection of metadata, including
system metrics and user input metadata. You can easily "put" or
"get" metadata for a workflow or a workflow step.

Retrieving metadata
-------------------
Workflow metadata can be retrieved with ``workflow.get_metadata(workflow_id)``.
For example:

.. code-block:: python

    @workflow.step
    def add(left: int, right: int) -> int:
        return left + right

    add.step(10, 20).run("add_example")

    workflow_metadata = workflow.get_metadata("add_example")

    assert workflow_metadata["status"] == "SUCCESSFUL"
    assert "user_metadata" in workflow_metadata
    assert "start_time" in workflow_metadata["stats"]
    assert "end_time" in workflow_metadata["stats"]

We can also retrieve metadata for individual workflow steps by
providing the step name:

.. code-block:: python

    add.options(name="add_step").step(10, 20).run("add_example_2")

    step_metadata = workflow.get_metadata("add_example_2", name="add_step")

    assert "name" in step_metadata
    assert step_metadata["step_options"]["step_options"] == "FUNCTION"
    assert step_metadata["step_options"]["max_retries"] == 3
    assert "user_metadata" in step_metadata
    assert "start_time" in workflow_metadata["stats"]
    assert "end_time" in workflow_metadata["stats"]

Attaching user-defined metadata
----------------------------
You can also add custom metadata to a workflow or a workflow step,
this is useful when you want to attach some extra information to the
workflow or workflow step. For example:

.. code-block:: python

    add.options(name="add_step", metadata={"step_k": "step_v"}).step(10, 20)\
    .run("add_example_3", metadata={"workflow_k": "workflow_v"})

    assert workflow.get_metadata("add_example_3")["user_metadata"] == {"workflow_k": "workflow_v"}
    assert workflow.get_metadata("add_example_3", name="add_step")["user_metadata"] == {"step_k": "step_v"}

**Note: user-defined metadata must be a python dictionary with values that are
json serializable.**

Metadata in Virtual Actor
-------------------------
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
    assert workflow.get_metadata("vid", "add")["stats"]["end_time"] >= \
           workflow.get_metadata("vid", "add")["stats"]["start_time"] + 1
    assert workflow.get_metadata("vid", "add_1")["stats"]["end_time"] >= \
           workflow.get_metadata("vid", "add_1")["stats"]["start_time"] + 1
    assert workflow.get_metadata("vid", "add_2")["stats"]["end_time"] >= \
           workflow.get_metadata("vid", "add_2")["stats"]["start_time"] + 1

**Notice that if there are multiple steps with the same name, a suffix
with a counter _n will be added automatically.**

And you can also do this in a nested fashion:

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

