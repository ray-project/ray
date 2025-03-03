Troubleshooting
==========

This page contains common issues and solutions for Compiled Graph execution.

Multiple executions with NumPy arrays
----------------
Ray chooses to zero-copy deserialize NumPy arrays when possible. For Compiled Graph execution,
this can lead to issues. 




