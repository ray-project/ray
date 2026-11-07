---
paths:
  - "python/ray/data/**/*.py"
---
<!-- General Ray Data code rules, distilled from code reviews. Add a rule by PR: general, one line, merged into an existing rule when one covers it. Rules for one module or format go in their own file. -->

## Design
- **Fix at the choke point.**
- **A new field on a shared type reaches everything that builds one.**
- **Write each piece of logic once.**
- **Shared code never names a subclass.**
- **Generic interfaces don't expose one library's types.**
- **Say concretely why a hook exists.**
- **A new feature works with every existing option, or rejects it.**
- **What planning promises matches what runs.**
- **Duplicates in the input stay duplicates.**
- **No process-global side effects** (`DataContext`, env vars).
- **State that grows with the input stays out of plans, task arguments and the
  driver.**
- **When porting code, justify every deviation from the original.**

## Code
- **No dead code.**
- **Don't repeat a condition across branches.**
- **Use the repo's plain idiom, not a clever one.**
- **A type-checker fix never changes runtime behavior.**
- **Read data your own code wrote strictly** (`d["key"]`, not `.get()` with a
  default).
- **Pass arguments by name; new parameters go last.**

## Names and types
- **One concept, one name.**
- **Every magic number is a named constant** with a comment saying why that
  value.
- **No `Any` when the real type is importable.**
- **Return a dataclass or `NamedTuple`, not a bare tuple.**

## Comments and docstrings
- **Every new non-empty file starts with a short header** saying what it's for,
  how it's built and which design choices it makes: enough to build something
  similar without reading the code.
- **A comment says why, in one sentence.**
- **Comments and docstrings stay true,** including ones outside the diff that
  describe the old behavior.
- **New classes and non-obvious methods get a docstring.**
- **A workaround gets a TODO naming the version that removes it.**

## Performance and memory
- **Size work by its peak memory,** not bytes on disk or item counts.
- **Do I/O in a few large reads, once.**
- **Don't copy or compute anything twice, or anything the output doesn't use.**
- **Test that an optimization skips the work it claims to,** not only the
  result.
- **Performance claims state the setup** and are measured at the small and
  large end.

## Errors and compatibility
- **One bad input fails only its own work.**
- **Retry only transient errors, and nothing that already retries.**
- **Error messages name what failed** and survive pickling.
- **A rewrite keeps the old path's inputs and outputs,** or the PR description
  says what changed.
- **A bug only old dependency versions have gets a doc note, not code.**
