# Bugbot Rules

## Rule: Unpickling in Ray Data readers
- If a changed datasource read path calls `ray.cloudpickle.loads` / `load` on bytes from a file, stream or service, wraps such bytes in `ArrowPythonObjectType` / `ArrowPythonObjectArray.from_objects`, or adds `allow_unsafe_unpickling()` without a user-facing opt-in that defaults off, post:

> ⚠️ Datasource code runs with unpickling blocked. `ray.cloudpickle.loads` is exempt and reserved for bytes Ray produced, so calling it on external bytes bypasses the guard; use the stdlib `pickle`. Wrapping external bytes in the pickled-object Arrow type defers the unpickle to consumers outside the guard. A reader that must unpickle exposes an explicit opt-in (like `read_numpy(allow_pickle=True)`), wraps only that call in `allow_unsafe_unpickling()`, and adds a reject test that plants a payload in real file bytes and proves it never runs.
