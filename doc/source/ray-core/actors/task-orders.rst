Actor Tasks Execution Orders
============

Synchronous, single-threaded actors execute tasks according
to their initial submission order. Any submitted task will not
execute until previously submitted tasks have finished execution.
Note that actor task execution could fail. Upon failure, the following
tasks submitted to the same actor will not execute until the failed
task is successfully retried or the retry is exhausted.

:ref:`Async or threaded actors <async-actors>` does not guarantee the
task execution order. For example, the system might execute a task
even though previously submitted tasks are pending execution.
