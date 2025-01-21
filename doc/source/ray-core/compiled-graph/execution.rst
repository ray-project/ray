Execution and failure semantics
===============================

Like classic Ray Core, Ray Compiled Graph propagates exceptions to the final output.
In particular:

- **Application exceptions**: If an application task throws an exception, Compiled Graph
  wraps the exception in a :class:`RayTaskError <ray.exceptions.RayTaskError>` and
  raises it when the caller calls :func:`ray.get() <ray.get>` on the result. The thrown
  exception inherits from both :class:`RayTaskError <ray.exceptions.RayTaskError>`
  and the original exception class.

- **System exceptions**: System exceptions include actor death or unexpected errors
  such as network errors. For actor death, Compiled Graph raises a
  :class:`ActorDiedError <ray.exceptions.ActorDiedError>`, and for other errors, it
  raises a :class:`RayChannelError <ray.exceptions.RayChannelError>`.

Ray Compiled Graph remains executable after application exceptions. However, Compiled Graph
automatically shuts down in the case of system exceptions. If an actor's death causes
the Compiled Graph to shut down, this shutdown doesn't affect the remaining actors. See the
following code as an example:

.. testcode::

    import ray
    from ray.dag import InputNode, MultiOutputNode

    @ray.remote
    class EchoActor:
    def echo(self, msg):
        return msg

    actors = [EchoActor.remote() for _ in range(4)]
    with InputNode() as inp:
        outputs = [actor.echo.bind(inp) for actor in actors]
        dag = MultiOutputNode(outputs)

    compiled_dag = dag.experimental_compile()
    # Kill one of the actors to simulate unexpected actor death.
    ray.kill(actors[0])
    ref = compiled_dag.execute(1)

    live_actors = []
    try:
        ray.get(ref)
    except ray.exceptions.ActorDiedError:
        # At this point, the Compiled Graph is shutting down.
        for actor in actors:
            try:
                # Check for live actors.
                ray.get(actor.echo.remote("ping"))
                live_actors.append(actor)
            except ray.exceptions.RayActorError:
                pass

    # Optionally, use the live actors to create a new Compiled Graph..
    assert live_actors == actors[1:]

Timeouts
--------

Some errors, such as network errors, require additional handling to avoid hanging.
To address these cases, Compiled Graph allows configurable timeouts for
``compiled_dag.execute()`` and :func:`ray.get() <ray.get>`.

The default timeout is 10 seconds for both. Set the following environment variables
to change the default timeout:

- ``RAY_CGRAPH_submit_timeout``: Timeout for ``compiled_dag.execute()``.
- ``RAY_CGRAPH_get_timeout``: Timeout for :func:`ray.get() <ray.get>`.

:func:`ray.get() <ray.get>` also has a timeout parameter to set timeout on a per-call basis.
