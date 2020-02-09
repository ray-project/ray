Fault Tolerance
===============

This document describes how Ray handles machine and process failures.

Tasks
-----

When a worker is executing a task, if the worker dies unexpectedly, either
because the process crashed or because the machine failed, Ray will rerun
the task (after a delay of several seconds) until either the task succeeds
or the maximum number of retries is exceeded. The default number of retries
is 4.

You can experiment with this behavior by running the following code.

.. code-block:: python

    import numpy as np
    import os
    import ray
    import time

    ray.init(ignore_reinit_error=True)

    @ray.remote(max_retries=1)
    def potentially_fail(failure_probability):
        time.sleep(0.2)
        if np.random.random() < failure_probability:
            os._exit(0)
        return 0

    for _ in range(3):
        try:
            # If this task crashes, Ray will retry it up to one additional
            # time. If either of the attempts succeeds, the call to ray.get
            # below will return normally. Otherwise, it will raise an
            # exception.
            ray.get(potentially_fail.remote(0.5))
            print('SUCCESS')
        except ray.exceptions.RayWorkerError:
            print('FAILURE')


Actors
------

If an actor process crashes unexpectedly, Ray will attempt to reconstruct the
actor process up to a maximum number of times. This value can be specified with
the ``max_reconstructions`` keyword, which by default is ``0``. If the maximum
number of reconstructions has been used up, then subsequent actor methods will
raise exceptions.

When an actor is reconstructed, its state will be recreated by rerunning its
constructor.

You can experiment with this behavior by running the following code.

.. code-block:: python

    import os
    import ray
    import time

    ray.init(ignore_reinit_error=True)

    @ray.remote(max_reconstructions=5)
    class Actor:
        def __init__(self):
            self.counter = 0

        def increment_and_possibly_fail(self):
            self.counter += 1
            time.sleep(0.2)
            if self.counter == 10:
                os._exit(0)
            return self.counter

    actor = Actor.remote()

    # The actor will be reconstructed up to 5 times. After that, methods will
    # raise exceptions. The actor is reconstructed by rerunning its
    # constructor. Methods that were executing when the actor died will also
    # raise exceptions.
    for _ in range(100):
        try:
            counter = ray.get(actor.increment_and_possibly_fail.remote())
            print(counter)
        except ray.exceptions.RayActorError:
            print('FAILURE')

Actor Checkpointing
~~~~~~~~~~~~~~~~~~~

It's often important to be able to checkpoint the state of an actor in order to
recover that state in the event of a failure. This can be done by having the
actor inherit from the ``ray.actor.Checkpointable`` interface.

Several things are left to the application and are not done by Ray. The reason
for this is that applications will want to handle checkpointing in many
different ways.
- When to create checkpoints. The ``should_checkpoint`` method should determine
  this.
- What to checkpoint, how to serialize it, and where to store the checkpoint.
  The ``save_checkpoint`` method handles this.
- How to restore the actor state from a given checkpoint. The
  ``load_checkpoint`` method takes care of this.

Here is an example in which we checkpoint the actor state every other iteration.
In this example, we save the checkpoints in a local file. To make this work in
the multi-node setting, we would need to write to a distributed file system or
some globally accessible storage.

.. code-block:: python

    import os
    import pickle
    import ray
    import time

    ray.init(ignore_reinit_error=True)

    @ray.remote(max_reconstructions=5)
    class Actor(ray.actor.Checkpointable):
        def __init__(self):
            self.counter = 0

        def increment(self):
            self.counter += 1

        def get_counter(self):
            return self.counter

        def should_checkpoint(self, checkpoint_context):
            # If the counter is even, then we will save a checkpoint.
            return self.counter % 5 == 0

        def save_checkpoint(self, actor_id, checkpoint_id):
            # Write the counter state to a local file.
            print('Saving a checkpoint at counter={}.'.format(self.counter))
            with open('actor_checkpoint-' + checkpoint_id.hex(), 'wb') as f:
                # The application must determine how to serialize the actor
                # state. Here we use pickle.
                f.write(pickle.dumps(self.counter))

        def load_checkpoint(self, actor_id, available_checkpoints):
            latest_checkpoint_id = available_checkpoints[0].checkpoint_id
            filename = 'actor_checkpoint-' + latest_checkpoint_id.hex()
            with open(filename, 'rb') as f:
                # Load the actor state using pickle.
                self.counter = pickle.load(f)
            print('Loading checkpoint with counter={}.'.format(self.counter))

        def checkpoint_expired(self, actor_id, checkpoint_id):
            print('The checkpoint {} has expired'.format(checkpoint_id))

    actor = Actor.remote()

    num_iterations = 27
    for _ in range(num_iterations):
        ray.get(actor.increment.remote())

    # Kill the actor. It will restart from the most recent checkpoint (which
    # should be a multiple of 5).
    actor.__ray_kill__()

    while True:
        # Loop until the actor has restarted.
        try:
            counter = ray.get(actor.get_counter.remote())
            break
        except ray.exceptions.RayActorError:
            continue
    assert counter == (num_iterations // 5) * 5
