Anti-pattern: Forking new processes in application code
========================================================

**Summary:** Don't fork new processes in Ray application code-for example, in  
driver, tasks or actors. Instead, use "spawn" method to start new processes or use Ray 
tasks and actors to parallelize your workload

Ray manages the lifecycle of processes for you. Ray Objects, Tasks, and 
Actors manages sockets to communicate with the Raylet and the GCS. If you fork new 
processes in your application code, the processes could share the same sockets without 
any synchronization. This can lead to corrupted message and unexpected
behavior.

The solution is to:
1. use "spawn" method to start new processes so that parent process's 
memory space isn't copied to the child processes or
2. use Ray tasks and 
actors to parallelize your workload and let Ray to manage the lifecycle of the 
processes for you.

Code example
------------
.. literalinclude:: ../doc_code/anti_pattern_fork_new_processes.py
    :language: python

