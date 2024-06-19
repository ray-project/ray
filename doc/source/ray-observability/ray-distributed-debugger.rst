Ray Distributed Debugger
========================

The Ray Distributed Debugger is a VS Code extension that streamlines the debugging process with an interactive debugging experience. The Ray Debugger enables you to:

- **Break into remote tasks**: Set a breakpoint in any remote task. A breakpoint pauses execution and allows you to connect with VS Code for debugging.
- **Post-mortem debugging**: When Ray tasks fail with unhandled exceptions, Ray automatically freezes the failing task and waits for the Ray Debugger to attach, allowing you to inspect the state of the program at the time of the error.

Ray Distributed Debugger abstracts the complexities of debugging distributed systems for you to debug Ray applications more efficiently, saving time and effort in the development workflow.


Demo
====

.. raw:: html

    <div style="position: relative; height: 0; overflow: hidden; max-width: 100%; height: auto;">
        <iframe width="560" height="315" src="https://www.youtube.com/embed/EiGHHUXL0oI" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>
    </div>


Get started
===========

Click `here <https://www.anyscale.com/blog/ray-distributed-debugger?utm_source=ray_docs&utm_medium=docs&utm_campaign=promotion#download-for-free>`_ to download the Ray Debugger extension for free.


Setup Environment
~~~~~~~~~~~~~~~~~

Create a new virtual environment and install dependencies.

.. code-block:: python

    conda create -n myenv python=3.9
    conda activate myenv
    pip install "ray[default]" debugpy


Start a Ray Cluster
~~~~~~~~~~~~~~~~~~~

Run ray start `--head` to start a Ray Cluster.

.. code-block:: bash

    ray start --head


Register the cluster
~~~~~~~~~~~~~~~~~~~~

Add the Ray cluster `IP:PORT` to the cluster list. The default `IP:PORT` is `127.0.0.1:8265`, and you can change it when starting a new cluster. Make sure your current machine can access the IP and port.

.. image:: ./images/register-cluster.gif
    :align: center


Create a Ray Task
~~~~~~~~~~~~~~~~~

Create a file `job.py` with the following snippet. Add the `RAY_DEBUG` environment variable to enable Ray Debugger and add `breakpoint()` in the Ray task.

.. code-block:: python

    import ray
    import sys

    # Add RAY_DEBUG environment variable to enable Ray Debugger.
    ray.init(runtime_env={
        "env_vars": {"RAY_DEBUG": "1"}, 
    })

    @ray.remote
    def my_task(x):
        y = x * x
        breakpoint() # Add a breakpoint in the ray task.
        return y

    @ray.remote
    def post_mortem(x):
        x += 1
        raise Exception("An exception is raised")
        return x

    if len(sys.argv) == 1:
        ray.get(my_task.remote(10))
    else:
        ray.get(post_mortem.remote(10)) 


Setup Debugger Local Folder
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray Debugger needs to know the absolute path to the folder you submitted `job.py`. Use pwd command to get the submission path, and set the cluster's local folder to the path. For each cluster, you can set the local folder by clicking on the ⚙️ icon on the cluster item.

.. image:: ./images/setup-debugger.gif
    :align: center


Run Your Ray Application
~~~~~~~~~~~~~~~~~~~~~~~~

Start running your Ray application.

.. code-block:: bash

    python job.py


Attach to Paused Tasks
~~~~~~~~~~~~~~~~~~~~~~

When debugger hits a breakpoint

- The task enters a paused state.
- The terminal clearly indicates when the debugger pauses a task and waits for the debugger to attach.
- The paused task is listed in the Ray Debugger extension.
- Click on the paused task to attach the VS Code debugger.

.. image:: ./images/attach-paused-task.gif
    :align: center


Use the VS Code debugger
~~~~~~~~~~~~~~~~~~~~~~~~

Debug your Ray app just as you would when developing locally.


Post-mortem debugging
=====================

Use post-mortem debugging when Ray tasks encounter unhandled exceptions. In such cases, Ray automatically freezes the failing task, awaiting attachment by the Ray Debugger. This feature allows you to thoroughly investigate and inspect the program's state at the time of the error.

Run a Ray Task Raised Exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the same `job.py` created above with an additional argument raise-exception.
    
.. code-block:: bash

    python job.py raise-exception


Attach to Paused Tasks
~~~~~~~~~~~~~~~~~~~~~~

When the app throws an exception:

- The debugger freezes the task.
- The terminal clearly indicates when the debugger pauses a task is paused and waits for the debugger to attach.
- The paused task is listed in the Ray Debugger extension.
- You can click on a paused task to attach the VS Code debugger.

.. image:: ./images/post-moretem.gif
    :align: center


Use the VS Code Debugger
~~~~~~~~~~~~~~~~~~~~~~~~

Debug your Ray app just as you would when developing locally.


Feedback
=========

Join the `#ray-debugger <https://ray-distributed.slack.com/archives/C073MPGLAC9>`_ channel on the Ray Slack channel to get help.
