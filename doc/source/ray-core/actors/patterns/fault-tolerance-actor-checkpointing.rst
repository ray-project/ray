Pattern: Fault Tolerance with Actor Checkpointing
=================================================

Ray offers support for task and actor `fault tolerance <https://docs.ray.io/en/latest/ray-core/actors/fault-tolerance.html>`__. Specifically for actors, you can specify max_restarts to automatically enable restart for Ray actors. This means when your actor or the node hosting that actor crashed, the actor will be automatically reconstructed. However, this doesn’t provide ways for you to restore application level states in your actor. You checkpoint your actor periodically and read from the checkpoint if possible.

There are several ways to checkpoint:

- Write the state to local disk. This can cause trouble when actors are instantiated in multi-node clusters.
- Write the state to local disk and use cluster launcher to sync file across cluster.
- Write the state to Ray internal kv store. (This is an experimental feature and not suitable for large files).
- Write the state to a Ray actor placed on head node (using custom resource constraints).


Code example
------------

.. code-block:: python

    # max_restarts tells Ray to restart the actor infinite times
    # max_task_retries tells Ray to transparently retries actor call when you call ray.get(actor.process.remote())
    @ray.remote(max_restarts=-1, max_task_retries=-1)
    class ImmortalActor:
        def __init__(self):
            if os.path.exists("/tmp/checkpoint.pkl"):
                self.state = pickle.load(open("/tmp/checkpoint.pkl"))
            else:
                self.state = MyState()

        def process(self):
            ....

You can also achieve the same result just using regular Ray actors and some custom logic:

.. code-block:: python

    @ray.remote
    class Worker:
        def __init__(*args, **kwargs):
            self.state = {}

        def perform_task(*args, **kwargs):
            # This task might fail.
            ...

        def get_state():
            # Returns actor state.
            return self.state


        def load_state(state):
            # Loads actor state.
            self.state = state

    class Controller:
        def create_workers(num_workers):
            self.workers = [Worker.remote(...) for _ in range(num_workers)]

        def perform_task_with_fault_tol(max_retries, *args, **kwargs):
            # Perform tasks in a fault tolerant manner.
            for _ in range(max_retries):
                worker_states = ray.get(
                          [w.get_state.remote() for w in self.workers])
                success, result = self.perform_task_on_all_workers(
                          *args, **kwargs)
                if success:
                    return result
                else:
                    self.create_workers()
                    ray.get(
                                [w.load_state.remote(state)
                                      for w, state in zip(
                                          self.workers, worker_states)])
            return None


        def perform_task_on_all_workers(*args, **kwargs):
            futures = [
                   w.perform_task.remote(
                       *args, **kwargs) for w in self.workers]
                output = []
            unfinished = futures
            try:
                while len(unfinished) > 0:
                    finished, unfinished = ray.wait(unfinished)
                    output.extend(ray.get(finished))
            except RayActorError:
                return False, None

            return True, output
