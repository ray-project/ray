Long Running Tests
==================

This directory contains the long-running workloads which are intended to run
forever until they fail. To set up the project you need to run

.. code-block:: bash

    $ pip install anyscale
    $ anyscale init

Note that all the long running test is running inside virtual environment, tensorflow_p36

Running the Workloads
---------------------
The easiest approach to running these workloads is to use the
`Releaser`_ tool to run them with the command
``python cli.py suite:run long_running_tests``. By default, this
will start a session to run each workload in the Anyscale product
and kick them off.

To run the tests manually, you can also use the `Anyscale UI <https://www.anyscale.dev/>`. First run ``anyscale snapshot create`` from the command line to create a project snapshot. Then from the UI, you can launch an individual session and execute the run command for each test.

You can also start the workloads using the CLI with:

.. code-block:: bash

    $ anyscale start
    $ anyscale run test_workload --workload=<WORKLOAD_NAME> --wheel=<RAY_WHEEL_LINK>


Doing this for each workload will start one EC2 instance per workload and will start the workloads
running (one per instance). A list of
available workload options is available in the `ray_projects/project.yaml` file.


Debugging
---------
The primary method to debug the test while it is running is to view the logs and the dashboard from the UI. After the test has failed, you can still view the stdout logs in the UI and also inspect
the logs under ``/tmp/ray/session*/logs/`` and
``/tmp/ray/session*/logs/debug_state.txt``.

.. To check up on the workloads, run either
.. ``anyscale session --name="*" execute check-load``, which
.. will print the load on each machine, or
.. ``anyscale session --name="*" execute show-output``, which
.. will print the tail of the output for each workload.

Shut Down the Workloads
-----------------------

The instances running the workloads can all be killed by running
``anyscale stop <SESSION_NAME>``.

Adding a Workload
-----------------

To create a new workload, simply add a new Python file under ``workloads/`` and
add the workload in the run command in `ray-project/project.yaml`.

.. _`Releaser`: https://github.com/ray-project/releaser
