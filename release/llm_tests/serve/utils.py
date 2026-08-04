import ray
from ray import serve
from ray.exceptions import RayActorError
from ray.serve.context import _get_global_client


def shutdown_serve_and_wait_for_controller() -> None:
    """Shut Serve down and wait until the controller actor exits.

    ``serve.shutdown()`` may return after its client timeout while the controller is
    still finishing a healthy teardown. Waiting on its shutdown method gives us an
    object ref that resolves with ``RayActorError`` when the controller kills itself.
    """
    client = _get_global_client(raise_if_no_controller_running=False)
    controller = client._controller if client is not None else None

    serve.shutdown()
    if controller is None:
        return

    try:
        ray.get(controller.graceful_shutdown.remote())
    except RayActorError:
        pass
