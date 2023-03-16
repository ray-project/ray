Troubleshooting Failures
========================

Crashes
-------

If Ray crashed, you want to know why and what happened. Currently, this can be attributed to the following reasons:

- **Stressful workloads:** It is currently possible for a process to get
  marked as dead without actually having died. For example, workloads that
  create many many tasks in a short amount of time can sometimes interfere
  with the heartbeat mechanism that we use to check that processes are still alive.
  On the head node in the cluster, you can check the files
  ``/tmp/ray/session_*/logs/monitor*``. They will indicate which processes Ray has marked
  as dead (due to a lack of heartbeats).

- **Starting many actors:** Workloads that start a large number of actors all at
  once may exhibit problems when the processes (or libraries that they use)
  contend for resources. Similarly, a script that starts many actors over the
  lifetime of the application will eventually cause the system to run out of
  file descriptors. This is addressable, but currently we do not garbage collect
  actor processes until the script finishes.

- **Running out of file descriptors:** As a workaround, you may be able to
  increase the maximum number of file descriptors with a command like
  ``ulimit -n 65536``. If that fails, double check that the hard limit is
  sufficiently large by running ``ulimit -Hn``. If it is too small, you can
  increase the hard limit as follows (these instructions work on EC2).

    * Increase the hard ulimit for open file descriptors system-wide by running
      the following.

      .. code-block:: bash

        sudo bash -c "echo $USER hard nofile 65536 >> /etc/security/limits.conf"

    * Logout and log back in.


This document discusses some common problems that people run into when using Ray
as well as some known problems. If you encounter other problems, please
`let us know`_.

.. _`let us know`: https://github.com/ray-project/ray/issues
