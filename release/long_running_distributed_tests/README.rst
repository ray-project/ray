Long Running Distributed Tests
==============================

This directory contains the long-running multi-node workloads which are intended to run
forever until they fail. To set up the project you need to run

.. code-block:: bash

    $ pip install anyscale
    $ anyscale init

Running the Workloads
---------------------
Easiest approach is to use the `Anyscale UI <https://www.anyscale.dev/>`_. First run ``anyscale snapshot create`` from the command line to create a project snapshot. Then from the UI, you can launch an individual session and execute the test_workload command for each test. 

You can also start the workloads using the CLI with:

.. code-block:: bash

    $ anyscale start --ray-wheel=<RAY_WHEEL_LINK>
    $ anyscale run test_workload --workload=<WORKLOAD_NAME>


Doing this for each workload will start one EC2 instance per workload and will start the workloads
running (one per instance). A list of
available workload options is available in the `ray_projects/project.yaml` file.


Debugging
---------
The primary method to debug the test while it is running is to view the logs and the dashboard from the UI. After the test has failed, you can still view the stdout logs in the UI and also inspect
the logs under ``/tmp/ray/session*/logs/`` and
``/tmp/ray/session*/logs/debug_state.txt``.

Shut Down the Workloads
-----------------------

The instances running the workloads can all be killed by running
``anyscale stop <SESSION_NAME>``.

Adding a Workload
-----------------

To create a new workload, simply add a new Python file under ``workloads/`` and
add the workload in the run command in `ray-project/project.yaml`.
