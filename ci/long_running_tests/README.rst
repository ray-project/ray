Long Running Tests
==================

This directory contains scripts for starting long-running workloads which are
intended to run forever until they fail.

Running the Workloads
---------------------

To run the workloads, run ``./start_workloads.sh``. This will start one EC2
instance per  workload and will start the workloads running (one per instance).
Running the ``./start_workloads.sh`` script again will clean up any state from
the previous runs and will start the workloads again.

Check Workload Statuses
-----------------------

To check up on the workloads, run ``./check_workloads.sh``. This will print the
tail of each workload, and from the output you might be able to see if something
has failed.

To debug workloads that have failed, you may find it useful to ssh to the
relevant machine, attach to the tmux session (usually ``tmux a -t 0``), inspect
the logs under ``/tmp/ray/session*/logs/``, and also inspect
``/tmp/ray/session*/debug_state.txt``.

Adding a Workload
-----------------

To create a new workload, simply add a new Python file under ``workloads/``.
