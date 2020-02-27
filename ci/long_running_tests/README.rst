Long Running Tests
==================

This directory contains the long-running workloads which are intended to run
forever until they fail. To set up the project you need to run

.. code-block:: bash

    pip install anyscale
    anyscale project create


Running the Workloads
---------------------

You can start all the workloads with:

.. code-block:: bash

    anyscale session start -y run --workload="*" --wheel=https://s3-us-west-2.amazonaws.com/ray-wheels/releases/0.7.5/6da7eff4b20340f92d3fe1160df35caa68922a97/ray-0.7.5-cp36-cp36m-manylinux1_x86_64.whl

This will start one EC2 instance per workload and will start the workloads
running (one per instance). You can start a specific workload by specifying
its name as an argument ``--workload=`` instead of ``"*"``. A list of
available options is available via `any session start run --help`.


Check Workload Statuses
-----------------------

To check up on the workloads, run either
``anyscale session --name="*" execute check-load``, which
will print the load on each machine, or
``anyscale session --name="*" execute show-output``, which
will print the tail of the output for each workload.

To debug workloads that have failed, you may find it useful to ssh to the
relevant machine, attach to the tmux session (usually ``tmux a -t 0``), inspect
the logs under ``/tmp/ray/session*/logs/``, and also inspect
``/tmp/ray/session*/debug_state.txt``.

Shut Down the Workloads
-----------------------

The instances running the workloads can all be killed by running
``anyscale session stop --name "*"``.

Adding a Workload
-----------------

To create a new workload, simply add a new Python file under ``workloads/`` and
add the workload in the run command in `ray-project/project.yaml`.
