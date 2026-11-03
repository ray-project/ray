---
myst:
  html_meta:
    description: "Anti-pattern: global variables don't propagate between Ray workers; pass state explicitly or hold it in an actor."
---

# Anti-pattern: Using global variables to share state between tasks and actors

Don't use global variables to share state with tasks and actors. Instead, encapsulate the global variables in an actor and pass the actor handle to other tasks and actors.

Ray drivers, tasks, and actors run in different processes, so they don't share the same address space. If you modify global variables in one process, other processes don't see the changes.

Hold the global state in an actor's instance variables, and pass the actor handle to wherever your code reads or modifies the state. Ray doesn't support using class variables to manage state between instances of the same class. Ray instantiates each actor instance in its own process, so each actor has its own copy of the class variables.

## Code example

**Anti-pattern:**

```{literalinclude} ../doc_code/anti_pattern_global_variables.py
:language: python
:start-after: __anti_pattern_start__
:end-before: __anti_pattern_end__
```

**Better approach:**

```{literalinclude} ../doc_code/anti_pattern_global_variables.py
:language: python
:start-after: __better_approach_start__
:end-before: __better_approach_end__
```
