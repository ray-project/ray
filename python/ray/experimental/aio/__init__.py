import asyncio
import ray.cloudpickle as pickle
import contextvars

async def run(coro):
    while True:
        try:
            fut = coro.send(None)
        except StopIteration as val:
            return val.value

        if hasattr(fut, "_object_ref"):
            object_ref = fut._object_ref
            actor = object_ref.actor()
            # assert actor is not None, \
            #     "Awaiting ObjectRef from Task is unimplemented! "\
            #     "Please only await on actors."
            if actor is not None:
                result = await actor.__ray_execute_coroutine__.remote(coro,
                                                                      object_ref)
                return result

        # Needs to wait on an object other than fut, because fut is already
        # awaited on.
        # TODO: Chain Future instead of Event, to handle cancellations?
        # new_future = loop.create_future()
        # asyncio._chain_future(fut, new_future)
        # yield
        assert asyncio.get_running_loop() is fut.get_loop()
        event = asyncio.Event()

        def future_done(f):
            event.set()

        fut.add_done_callback(future_done)
        await event.wait()


work = {}
workflow_ctx = contextvars.ContextVar('workflow_ctx')


def queue_work(key, coro):
    global work
    if key in work:
        raise ValueError(f"Duplicated work key: {key}")
    work[key] = pickle.dumps(coro)
    coro.close()


async def execute_work(key):
    # import pdb; pdb.set_trace()
    stored_coro = work.get(key)
    if stored_coro is None:
        raise ValueError(f"Work key not found: {key}")
    coro = pickle.loads(stored_coro)
    workflow_ctx.set({"undo_callbacks": []})

    while True:
        try:
            fut = coro.send(None)
        except StopIteration as val:
            work.pop(key)
            return val.value
        except Exception as ex:


        # Checkpoints work.
        if hasattr(fut, "_object_ref"):
            work[key] = pickle.dumps(coro)

        assert asyncio.get_running_loop() is fut.get_loop()
        event = asyncio.Event()

        def future_done(f):
            event.set()

        fut.add_done_callback(future_done)
        await event.wait()

        # Checkpoints work.
        if hasattr(fut, "_object_ref"):
            work[key] = pickle.dumps(coro)


def demo_workflow(key, coro):
    print("\n\033[1mStarting workflow with coroutine checkpointing:\033[0m")
    queue_work(key, coro)
    num_breaks = 0
    while True:
        try:
            asyncio.run(execute_work(key))
        except KeyboardInterrupt:
            num_breaks += 1
            time = "time" if num_breaks == 1 else "times"
            if num_breaks == 3:
                print(f"Caught keyboard interrupt {num_breaks} {time}. "
                      "Exiting ...")
                break
            else:
                print(f"Caught keyboard interrupt {num_breaks} {time}. "
                      "Restarting work ...")
            continue

        # Finished execute work
        break
