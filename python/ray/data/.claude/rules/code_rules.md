---
paths:
  - "python/ray/data/**/*.py"
---
<!-- General Ray Data code rules, distilled from code reviews. Add a rule by PR: general, one line, merged into an existing rule when one covers it. Rules for one module or format go in their own file. -->

## Design
- **Fix a bug in the shared code every caller uses,** not only in the caller
  that showed it, and look for other paths that do the same thing.
- **The design is modular and extensible and doesn't repeat logic.**
- **Prefer overriding a method in the subclass to special-casing the subclass
  in shared code.**
- **Clearly justify every new function or class.**
- **A new feature doesn't conflict with existing features unless intended,**
  and an intended conflict raises an error.
- **What planning promises matches what runs.**
- **Change process-global state (`DataContext`, env vars) only on purpose,**
  and say why in the PR description.
- **State that grows with the input stays out of plans, task arguments and the
  driver** (e.g. a read task gets its own files' paths, not all of them).
- **When porting code, justify every deviation from the original.**

## Code
- **No dead code.**
- **Don't repeat a condition across branches.**
- **Use the repo's plain idiom, not a clever one.**
- **Silence a type-checker error with types** (annotations, `cast`), not new
  runtime checks; if the error is a real bug, fix it as one.
- **Read a key your code always writes with `d["key"]`;** use `.get()` with a
  default only when the key can legitimately be missing.
- **Pass arguments by name; new parameters go last.**

## Names and types
- **One concept, one name.**
- **Every magic number is a named constant** with a comment saying why that
  value.
- **No `Any` when the real type is importable.**
- **Return a dataclass or `NamedTuple`, not a bare tuple,** from public or
  widely used functions.

## Comments and docstrings
- **Every new non-empty file starts with a short header** saying what it's for,
  how it's built and which design choices it makes: enough to build something
  similar without reading the code. Where data flows through it, add an ASCII
  diagram.
- **Comment only non-trivial code,** saying why in one sentence.
- **Comments and docstrings stay true,** including ones outside the diff that
  describe the old behavior.
- **New classes and non-obvious methods get a docstring.**
- **A workaround gets a TODO naming the version that removes it.**

## Performance and memory
- **Don't copy or compute anything twice, or anything the output doesn't use.**
- **Test that an optimization skips the work it claims to,** not only the
  result.
- **Performance claims state the setup** and are measured at the small and
  large end.

## Errors and compatibility
- **Errors are isolated and verifiable.**
- **Avoid retry storms:** retry only transient errors, with backoff, and never
  around code that already retries.
- **Error messages name what failed** and survive pickling.
- **A rewrite keeps the old path's inputs and outputs,** or the PR description
  says what changed.
- **A bug only old dependency versions have gets a doc note, not code.**
