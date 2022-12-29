.. _monte-carlo-pi:

Monte Carlo Estimation of π
===========================

This tutorial shows you how to estimate the value of π using a `Monte Carlo method <https://en.wikipedia.org/wiki/Monte_Carlo_method>`_
that works by randomly sampling points within a 2x2 square.
We can use the proportion of the points that are contained within the unit circle centered at the origin
to estimate the ratio of the area of the circle to the area of the square.
Given that we know the true ratio to be π/4, we can multiply our estimated ratio by 4 to approximate the value of π.
The more points that we sample to calculate this approximation, the closer the value should be to the true value of π.

.. image:: ../images/monte_carlo_pi.png

We use Ray :ref:`tasks <ray-remote-functions>` to distribute the work of sampling and Ray :ref:`actors <ray-remote-classes>` to track the progress of these distributed sampling tasks.
The code can run on your laptop and can be easily scaled to large :ref:`clusters <cluster-index>` to increase the accuracy of the estimate.

To get started, install Ray via ``pip install -U ray``. See :ref:`Installing Ray <installation>` for more installation options.

Starting Ray
------------
First, let's include all modules needed for this tutorial and start a local Ray cluster with :ref:`ray.init() <ray-init-ref>`:

.. literalinclude:: ../doc_code/monte_carlo_pi.py
    :language: python
    :start-after: __starting_ray_start__
    :end-before: __starting_ray_end__

.. note::

  In recent versions of Ray (>=1.5), ``ray.init()`` is automatically called on the first use of a Ray remote API.


Defining the Progress Actor
---------------------------
Next, we define a Ray actor that can be called by sampling tasks to update progress.
Ray actors are essentially stateful services that anyone with an instance (a handle) of the actor can call its methods.

.. literalinclude:: ../doc_code/monte_carlo_pi.py
    :language: python
    :start-after: __defining_actor_start__
    :end-before: __defining_actor_end__

We define a Ray actor by decorating a normal Python class with :ref:`ray.remote <ray-remote-ref>`.
The progress actor has ``report_progress()`` method that will be called by sampling tasks to update their progress individually
and ``get_progress()`` method to get the overall progress.

Defining the Sampling Task
--------------------------
After our actor is defined, we now define a Ray task that does the sampling up to ``num_samples`` and returns the number of samples that are inside the circle.
Ray tasks are stateless functions. They execute asynchronously, and run in parallel.

.. literalinclude:: ../doc_code/monte_carlo_pi.py
    :language: python
    :start-after: __defining_task_start__
    :end-before: __defining_task_end__

To convert a normal Python function as a Ray task, we decorate the function with :ref:`ray.remote <ray-remote-ref>`.
The sampling task takes a progress actor handle as an input and reports progress to it.
The above code shows an example of calling actor methods from tasks.

Creating a Progress Actor
-------------------------
Once the actor is defined, we can create an instance of it.

.. literalinclude:: ../doc_code/monte_carlo_pi.py
    :language: python
    :start-after: __creating_actor_start__
    :end-before: __creating_actor_end__

To create an instance of the progress actor, simply call ``ActorClass.remote()`` method with arguments to the constructor.
This creates and runs the actor on a remote worker process.
The return value of ``ActorClass.remote(...)`` is an actor handle that can be used to call its methods.

Executing Sampling Tasks
------------------------
Now the task is defined, we can execute it asynchronously.

.. literalinclude:: ../doc_code/monte_carlo_pi.py
    :language: python
    :start-after: __executing_task_start__
    :end-before: __executing_task_end__

We execute the sampling task by calling ``remote()`` method with arguments to the function.
This immediately returns an ``ObjectRef`` as a future
and then executes the function asynchronously on a remote worker process.

Calling the Progress Actor
--------------------------
While sampling tasks are running, we can periodically query the progress by calling the actor ``get_progress()`` method.

.. literalinclude:: ../doc_code/monte_carlo_pi.py
    :language: python
    :start-after: __calling_actor_start__
    :end-before: __calling_actor_end__

To call an actor method, use ``actor_handle.method.remote()``.
This invocation immediately returns an ``ObjectRef`` as a future
and then executes the method asynchronously on the remote actor process.
To fetch the actual returned value of ``ObjectRef``, we use the blocking :ref:`ray.get() <ray-get-ref>`.

Calculating π
-------------
Finally, we get number of samples inside the circle from the remote sampling tasks and calculate π.

.. literalinclude:: ../doc_code/monte_carlo_pi.py
    :language: python
    :start-after: __calculating_pi_start__
    :end-before: __calculating_pi_end__

As we can see from the above code, besides a single ``ObjectRef``, :ref:`ray.get() <ray-get-ref>` can also take a list of ``ObjectRef`` and return a list of results.

If you run this tutorial, you will see output like:

.. code-block:: text

 Progress: 0%
 Progress: 15%
 Progress: 28%
 Progress: 40%
 Progress: 50%
 Progress: 60%
 Progress: 70%
 Progress: 80%
 Progress: 90%
 Progress: 100%
 Estimated value of π is: 3.1412202
