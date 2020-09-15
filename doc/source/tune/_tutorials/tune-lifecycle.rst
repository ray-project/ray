.. _tune-lifecycle:

What happens when you call tune.run?
====================================

Ever wonder what happens when you call ``tune.run``?

What happens in ``tune.run``?
-----------------------------

**Short:** The function/trainable is evaluated multiple times in parallel with different hyperparameters.

**Long:** tune.run takes the config and search algorithm and generates a couple hyperparameter configurations. A trial is the logical representation of a hyperparameter configuration.

Each trial is defined by a resource request, a hyperparameter configuration, id, and other parameters given in tune.run.

Your Tune execution consists of a "driver process" and many "worker processes". The driver process is the python process in which you call tune.run (henceforth known as the **Tune Driver**, while "worker processes" are what executes the trial.

Before running a trial, the Tune Driver will check whether there are available resources on the cluster (link - what are resources). It will compare the available resources with the resources required by the trial.

If there is space on the cluster, then the Tune Driver will start a Ray actor (worker). This ray actor will be scheduled and executed on some node where the resources are available. (link - what is a Ray actor)

The Ray actor is given a copy of the Trainable (class or function, passed in via tune.run) to be executed (link - cloudpickle). While the Trainable is executing (link - how does the Ray actor execute the function), the Tune process, which lives on the driver node, communicates with each actor to receive intermediate training results and pause/stop actors. See the Ray Internals guide for more information <LINK>.

When the function terminates (or is stopped), the actor is also terminated.

Lifecycle of a trial
--------------------

There are a couple explicit stages for a trial's life cycle.

-> Initialization (generation): A trial is first generated as a hyperparameter sample, and its parameters are configured according to what was provided in tune.run. Trials are then placed into a queue to be executed (with status PENDING)

-> PENDING: A pending trial is a trial to be executed on the machine. Every trial is configured with resource values. Whenever the trial’s resource values are available, tune will run the trial (by starting a ray actor holding the config and the training function.

-> RUNNING: A running trial is backed by a Ray Actor <link>. There can be multiple running trials in parallel. See "How does the Ray actor execute the function?"

-> ERRORED: If a running trial throws an exception, Tune will catch that exception and mark the trial as errored. Note that exceptions can be propagated from an actor to the main Tune driver process. If max_retries is set, Tune will set the trial back into "PENDING" and later start it from the last checkpoint.

-> TERMINATED: A trial is terminated if it is stopped by a Stopper/Scheduler. If using the Function API, the trial is terminated when the function stops.

-> PAUSED: A trial can be paused by a Scheduler. This means that the trial’s actor will be stopped. A paused trial can later be resumed from the most recent checkpoint.

How does the Ray actor execute the function?
--------------------------------------------

The function is executed on a separate thread. Whenever tune.report is called, the execution thread is paused and waits for the driver to pull a result.

The driver is notified when a "result" is ready (i.e., ray.wait()). After the result is pulled to the driver, the remote worker’s execution thread will automatically resume <LINK TO CODE>

What is a Ray cluster? What is a head node?
-------------------------------------------

A ray cluster can be a laptop, a single server, or multiple servers.

There are three ways of starting a "ray cluster".
* Implicitly via ray.init() (or tune.run).
  - ray.init() starts a 1 node ray cluster on your laptop/machine, and it becomes the "head node".
  - You are automatically connected to the ray cluster.
  - When the process calling ray.init terminates, the ray cluster will also terminate.
* Explicitly via CLI (ray start).
  - This command also starts a 1 node ray cluster on your laptop/machine, and it becomes the "head node". You can expand the number of nodes in the cluster by calling {...}.
  - Any process that runs ‘ray.init(address=...)’ on any of the cluster machines will connect to the ray cluster.
* Explicitly via the cluster launcher (ray up).
  - This command starts a cluster on the cloud, and there is a designated separate "head node" and designated worker nodes.
  - Any process that runs ‘ray.init(address=...)’ on any of the cluster nodes will connect to the ray cluster.
  - Note that calling ray.init on your laptop will not work.

The driver process is the process that calls "ray.init(...)" or "tune.run" (which calls ray.init() underneath the hood). It is typical to call ray.init(...) on the head node of the cluster.

So therefore, the Tune driver process runs on the node where you run your script (which calls tune.run), while Ray Tune trial ‘actors’ run on any node (either the head or the worker aka non-head nodes).
