Anti-pattern: Using global variables to share state between tasks and actors
============================================================================

**TLDR:** Don't use global variables to share state with tasks and actors. Instead, encapsulate the global variables in an actor and pass the actor handle to other tasks and actors.

Ray drivers, tasks and actors are running in
different processes, so they don’t share the same address space.
This means that if you modify global variables
in one process, changes are not reflected in other processes.

The solution is to use an actor's instance variables to hold the global state and pass the actor handle to places where the state needs to be modified or accessed.
Note that using class variables to manage state between instances of the same class is not supported.
Each actor instance is instantiated in its own process, so each actor will have its own copy of the class variables.

Code example
------------

**Anti-pattern:**

.. literalinclude:: ../doc_code/anti_pattern_global_variables.py
    :language: python
    :start-after: __anti_pattern_start__
    :end-before: __anti_pattern_end__

**Better approach:**

.. literalinclude:: ../doc_code/anti_pattern_global_variables.py
    :language: python
    :start-after: __better_approach_start__
    :end-before: __better_approach_end__
