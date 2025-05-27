import asyncio
import importlib
import sys

from typing import Coroutine


def import_attr(full_path: str, *, reload_module: bool = False):
    """Given a full import path to a module attr, return the imported attr.

    If `reload_module` is set, the module will be reloaded using `importlib.reload`.

    For example, the following are equivalent:
        MyClass = import_attr("module.submodule:MyClass")
        MyClass = import_attr("module.submodule.MyClass")
        from module.submodule import MyClass

    Returns:
        Imported attr
    """
    if full_path is None:
        raise TypeError("import path cannot be None")

    if ":" in full_path:
        if full_path.count(":") > 1:
            raise ValueError(
                f'Got invalid import path "{full_path}". An '
                "import path may have at most one colon."
            )
        module_name, attr_name = full_path.split(":")
    else:
        last_period_idx = full_path.rfind(".")
        module_name = full_path[:last_period_idx]
        attr_name = full_path[last_period_idx + 1 :]

    module = importlib.import_module(module_name)
    if reload_module:
        importlib.reload(module)
    return getattr(module, attr_name)


def get_or_create_event_loop() -> asyncio.AbstractEventLoop:
    """Get a running async event loop if one exists, otherwise create one.

    This function serves as a proxy for the deprecating get_event_loop().
    It tries to get the running loop first, and if no running loop
    could be retrieved:
    - For python version <3.10: it falls back to the get_event_loop
        call.
    - For python version >= 3.10: it uses the same python implementation
        of _get_event_loop() at asyncio/events.py.

    Ideally, one should use high level APIs like asyncio.run() with python
    version >= 3.7, if not possible, one should create and manage the event
    loops explicitly.
    """
    vers_info = sys.version_info
    if vers_info.major >= 3 and vers_info.minor >= 10:
        # This follows the implementation of the deprecating `get_event_loop`
        # in python3.10's asyncio. See python3.10/asyncio/events.py
        # _get_event_loop()
        try:
            loop = asyncio.get_running_loop()
            assert loop is not None
            return loop
        except RuntimeError as e:
            # No running loop, relying on the error message as for now to
            # differentiate runtime errors.
            assert "no running event loop" in str(e)
            return asyncio.get_event_loop_policy().get_event_loop()

    return asyncio.get_event_loop()


_BACKGROUND_TASKS = set()


def run_background_task(coroutine: Coroutine) -> asyncio.Task:
    """Schedule a task reliably to the event loop.

    This API is used when you don't want to cache the reference of `asyncio.Task`.
    For example,

    ```
    get_event_loop().create_task(coroutine(*args))
    ```

    The above code doesn't guarantee to schedule the coroutine to the event loops

    When using create_task in a  "fire and forget" way, we should keep the references
    alive for the reliable execution. This API is used to fire and forget
    asynchronous execution.

    https://docs.python.org/3/library/asyncio-task.html#creating-tasks
    """
    task = get_or_create_event_loop().create_task(coroutine)
    # Add task to the set. This creates a strong reference.
    _BACKGROUND_TASKS.add(task)

    # To prevent keeping references to finished tasks forever,
    # make each task remove its own reference from the set after
    # completion:
    task.add_done_callback(_BACKGROUND_TASKS.discard)
    return task
