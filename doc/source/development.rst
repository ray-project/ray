Development Tips
================

If you are doing development on the Ray codebase, the following tips may be
helpful.

1. **Speeding up compilation:** Be sure to install Ray with

   .. code-block:: shell

     cd ray/python
     python setup.py develop

   (as opposed to ``python setup.py install``). When you do the "install"
   version, files will be copied from the Ray directory to a directory of Python
   packages (often something like
   ``/home/ubuntu/anaconda3/lib/python3.6/site-packages/ray``). This means that
   changes you make to files in the Ray directory will not have any effect.
   However, when you run the "develop" version, no files will be copied and so
   any changes you make to Python files will immediately take effect without
   rerunning ``setup.py``.

   If you run into **Permission Denied** errors when running ``setup.py``, you
   can try doing ``python setup.py develop --user``. You may also need to run
   something like ``sudo chown -R $USER /home/ubuntu/anaconda3`` (substituting
   in the appropriate path).

   If you make changes to the C++ files, you will need to recompile them.
   However, you do not need to rerun ``setup.py``. Instead, you can recompile
   much more quickly by doing

   .. code-block:: shell

     cd ray/python/ray/core
     make -j8

2. **Starting processes in a debugger:** When processes are crashing, it is
   often useful to start them in a debugger (``gdb`` on Linux or ``lldb`` on
   MacOS). See the latest discussion about how to do this `here`_.

3. **Running tests locally:** Suppose that one of the tests (e.g.,
   ``runtest.py``) is failing. You can run that test locally by running
   ``python test/runtest.py``. However, doing so will run all of the tests which
   can take a while. To run a specific test that is failing, you can do

   .. code-block:: shell

     cd ray
     python test/runtest.py APITest.testKeywordArgs

   When running tests, usually only the first test failure matters. A single
   test failure often triggers the failure of subsequent tests in the same
   script.

4. **Running linter locally:** To run the Python linter on a specific file, run
   something like ``flake8 ray/python/ray/worker.py``. You may need to first run
   ``pip install flake8``.

5. **Inspecting Redis shards by hand:** To inspect the primary Redis shard by
   hand, you can query it with commands like the following.

   .. code-block:: python

     r_primary = ray.worker.global_worker.redis_client
     r_primary.keys("*")

   To inspect other Redis shards, you will need to create a new Redis client.
   For example (assuming the relevant IP address is ``127.0.0.1`` and the
   relevant port is ``1234``), you can do this as follows.

   .. code-block:: python

     import redis
     r = redis.StrictRedis(host='127.0.0.1', port=1234)

   You can find a list of the relevant IP addresses and ports by running

   .. code-block:: python

     r_primary.lrange('RedisShards', 0, -1)

.. _`here`: https://github.com/ray-project/ray/issues/108
