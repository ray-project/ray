Anti-pattern: Creating new Processes in Application Code
========================================================

**Summary:** Don't create new processes in application code. Instead, use Ray tasks and 
actors to parallelize your workload.

Ray manages the lifecycle of processes for you. Ray Objects, Tasks, and 
Actors manages sockets to communicate with the Raylet and the GCS. If you create new 
processes in your application code, the processes could share the same sockets without 
any synchronization. This can lead to corrupted message and unexpected
behavior.

The solution is to use Ray tasks and actors to parallelize your workload and let Ray to 
manage the lifecycle of the processes for you.

Code example
------------

**Anti-pattern:**

.. literalinclude:: ../doc_code/anti_pattern_create_new_processes.py
    :language: python
    :start-after: __anti_pattern_start__
    :end-before: __anti_pattern_end__

**Better approach:**

.. literalinclude:: ../doc_code/anti_pattern_create_new_processes.py
    :language: python
    :start-after: __better_approach_start__
    :end-before: __better_approach_end__

