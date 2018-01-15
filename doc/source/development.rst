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

   If you make changes to the C++ files, you will need to recompile them.
   However, you do not need to rerun ``setup.py``. Instead, you can recompile
   much more quickly by doing

   .. code-block:: shell

     cd ray/python/ray/core
     make -j8

2. **Starting processes in a debugger:** When processes are crashing, it is
   often useful to start them in a debugger (``gdb`` on Linux or ``lldb`` on
   MacOS). See the latest discussion about how to do this `here`_.

.. _`here`: https://github.com/ray-project/ray/issues/108
