.. warning::
    This page is under construction!

.. _ray-job-cli-under-construction:

CLI
^^^

The easiest way to get started with Ray job submission is to use the Job Submission CLI. 

Jump to the :ref:`API Reference<ray-job-submission-cli-ref>`, or continue reading for a walkthrough.


Using the CLI on a local cluster
""""""""""""""""""""""""""""""""

First, start a local Ray cluster (e.g. with ``ray start --head``) and open a terminal (on the head node, which is your local machine).  

Next, set the :code:`RAY_ADDRESS` environment variable:

.. code-block:: bash

    export RAY_ADDRESS="http://127.0.0.1:8265"

This tells the jobs CLI how to find your Ray cluster.  Here we are specifying port ``8265`` on the head node, the port that the Ray Dashboard listens on.  
(Note that this port is different from the port used to connect to the cluster via :ref:`Ray Client <ray-client>`, which is ``10001`` by default.)

Now you are ready to use the CLI.  
Here are some examples of CLI commands from the Quick Start example and their output:

.. code-block::

    ❯ ray job submit --runtime-env-json='{"working_dir": "./", "pip": ["requests==2.26.0"]}' -- python script.py
    2021-12-01 23:04:52,672	INFO cli.py:25 -- Creating JobSubmissionClient at address: http://127.0.0.1:8265
    2021-12-01 23:04:52,809	INFO sdk.py:144 -- Uploading package gcs://_ray_pkg_bbcc8ca7e83b4dc0.zip.
    2021-12-01 23:04:52,810	INFO packaging.py:352 -- Creating a file package for local directory './'.
    2021-12-01 23:04:52,878	INFO cli.py:105 -- Job submitted successfully: raysubmit_RXhvSyEPbxhcXtm6.
    2021-12-01 23:04:52,878	INFO cli.py:106 -- Query the status of the job using: `ray job status raysubmit_RXhvSyEPbxhcXtm6`.

    ❯ ray job status raysubmit_RXhvSyEPbxhcXtm6
    2021-12-01 23:05:00,356	INFO cli.py:25 -- Creating JobSubmissionClient at address: http://127.0.0.1:8265
    2021-12-01 23:05:00,371	INFO cli.py:127 -- Job status for 'raysubmit_RXhvSyEPbxhcXtm6': PENDING.
    2021-12-01 23:05:00,371	INFO cli.py:129 -- Job has not started yet, likely waiting for the runtime_env to be set up.

    ❯ ray job status raysubmit_RXhvSyEPbxhcXtm6
    2021-12-01 23:05:37,751	INFO cli.py:25 -- Creating JobSubmissionClient at address: http://127.0.0.1:8265
    2021-12-01 23:05:37,764	INFO cli.py:127 -- Job status for 'raysubmit_RXhvSyEPbxhcXtm6': SUCCEEDED.
    2021-12-01 23:05:37,764	INFO cli.py:129 -- Job finished successfully.

    ❯ ray job logs raysubmit_RXhvSyEPbxhcXtm6
    2021-12-01 23:05:59,026	INFO cli.py:25 -- Creating JobSubmissionClient at address: http://127.0.0.1:8265
    2021-12-01 23:05:23,037	INFO worker.py:851 -- Connecting to existing Ray cluster at address: 127.0.0.1:6379
    (pid=runtime_env) 2021-12-01 23:05:23,212	WARNING conda.py:54 -- Injecting /Users/jiaodong/Workspace/ray/python to environment /tmp/ray/session_2021-12-01_23-04-44_771129_7693/runtime_resources/conda/99305e1352b2dcc9d5f38c2721c7c1f1cc0551d5 because _inject_current_ray flag is on.
    (pid=runtime_env) 2021-12-01 23:05:23,212	INFO conda.py:328 -- Finished setting up runtime environment at /tmp/ray/session_2021-12-01_23-04-44_771129_7693/runtime_resources/conda/99305e1352b2dcc9d5f38c2721c7c1f1cc0551d5
    (pid=runtime_env) 2021-12-01 23:05:23,213	INFO working_dir.py:85 -- Setup working dir for gcs://_ray_pkg_bbcc8ca7e83b4dc0.zip
    1
    2
    3
    4
    5
    2.26.0

    ❯ ray job list
    {'raysubmit_AYhLMgDJ6XBQFvFP': JobInfo(status='SUCCEEDED', message='Job finished successfully.', error_type=None, start_time=1645908622, end_time=1645908623, metadata={}, runtime_env={}),
    'raysubmit_su9UcdUviUZ86b1t': JobInfo(status='SUCCEEDED', message='Job finished successfully.', error_type=None, start_time=1645908669, end_time=1645908670, metadata={}, runtime_env={})}

.. warning::

    When using the CLI, do not wrap the entrypoint command in quotes.  For example, use 
    ``ray job submit --working_dir="." -- python script.py`` instead of ``ray job submit --working_dir="." -- "python script.py"``.
    Otherwise you may encounter the error ``/bin/sh: 1: python script.py: not found``.

.. tip::

    If your job is stuck in `PENDING`, the runtime environment installation may be stuck.
    (For example, the `pip` installation or `working_dir` download may be stalled due to internet issues.)
    You can check the installation logs at `/tmp/ray/session_latest/logs/runtime_env_setup-*.log` for details.

Using the CLI on a remote cluster
"""""""""""""""""""""""""""""""""

Above, we ran the "Quick Start" example on a local Ray cluster.  When connecting to a `remote` cluster via the CLI, you need to be able to access the Ray Dashboard port of the cluster over HTTP.

One way to do this is to port forward ``127.0.0.1:8265`` on your local machine to ``127.0.0.1:8265`` on the head node. 
If you started your remote cluster with the :ref:`Ray Cluster Launcher <ref-cluster-quick-start>`, then the port forwarding can be set up automatically using the ``ray dashboard`` command (see :ref:`monitor-cluster` for details).

To use this, run the following command on your local machine, where ``cluster.yaml`` is the configuration file you used to launch your cluster:

.. code-block:: bash

    ray dashboard cluster.yaml

Once this is running, check that you can view the Ray Dashboard in your local browser at ``http://127.0.0.1:8265``.  

Next, set the :code:`RAY_ADDRESS` environment variable:

.. code-block:: bash

    export RAY_ADDRESS="http://127.0.0.1:8265"

(Note that this port is different from the port used to connect to the cluster via :ref:`Ray Client <ray-client>`, which is ``10001`` by default.)

Now you will be able to use the Jobs CLI on your local machine as in the example above to interact with your remote Ray cluster.

Using the CLI on Kubernetes
"""""""""""""""""""""""""""

The instructions above still apply, but you can achieve the dashboard port forwarding using ``kubectl port-forward``:
https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster/

Alternatively, you can set up Ingress to the dashboard port of the cluster over HTTP: https://kubernetes.io/docs/concepts/services-networking/ingress/
