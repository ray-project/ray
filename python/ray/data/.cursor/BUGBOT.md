# Bugbot Rules

## Rule: Unpickling in Ray Data readers
- If a change adds a place that runs datasource, scanner, reader, indexer or metadata-reader code that can open external bytes (a driver call, planner op, optimizer rule, actor method or task), or starts a thread that does, and that code is not under `forbid_untrusted_unpickling()` / `guard_iterator` (threads: `contextvars.copy_context().run`), post:

> ⚠️ This runs code that reads external bytes outside the unpickling guard. Wrap it in `forbid_untrusted_unpickling()` (or `guard_iterator` for a generator), and start helper threads with `contextvars.copy_context().run` so they inherit the flag.

- If a changed datasource read path calls `allow_unsafe_unpickling()` around anything other than a `ray.get` of Ray's own objects (it must never cover objects built from a file, stream or service), decodes data in a thread it starts, wraps such bytes in `ArrowPythonObjectType` / `ArrowPythonObjectArray.from_objects`, or adds `allow_unsafe_unpickling()` without a user-facing opt-in that defaults off, post:

> ⚠️ Datasource code runs with unpickling blocked on the thread that runs it. Lifting the guard around `ray.get` is reserved for objects Ray produced, and threads the reader starts run unguarded. Wrapping external bytes in the pickled-object Arrow type defers the unpickle to consumers outside the guard. A reader that must unpickle exposes an explicit opt-in (like `read_numpy(allow_pickle=True)`), wraps only that call in `allow_unsafe_unpickling()`, and adds a reject test that plants a payload in real file bytes and proves it never runs.
