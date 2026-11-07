---
paths:
  - "python/ray/data/**/*.py"
---
<!-- General Ray Data code rules, distilled from code reviews. Add a rule by PR: general, one or two lines, merged into an existing rule when one covers it. Rules for one module or format go in their own file. -->

## Design
- **Fix at the choke point.** Grep for every producer and consumer of what you
  changed, legacy paths included.
- **A new field on a shared type reaches everything that builds one:**
  `dataclasses.replace`, not field-by-field copies.
- **Write each piece of logic once.** To vary one step of a parent method, add
  a small overridable hook whose default is today's behavior.
- **Shared code never names a subclass.** The class declares a capability;
  behavior one implementation has lives in its override.
- **Generic interfaces don't expose one library's types.**
- **Say concretely why a hook exists:** what can't happen where, and what
  breaks if it does ("needs I/O, and the planner runs it serially without
  retries", not "override when needed").
- **A new feature works with every existing option that reaches it,** or
  rejects or documents the rest.
- **What planning promises matches what runs:** the schema, sizes and row
  counts reported before execution are what every output block has.
- **Duplicates in the input stay duplicates;** don't key by something that can
  repeat.
- **No process-global side effects** (`DataContext`, env vars). Read settings
  on the driver and pass them in; workers don't see the driver's env changes.
- **State that grows with the input stays out of plans, task arguments and the
  driver;** put large shared state once and pass a reference.
- **When porting code, justify every deviation from the original.**

## Code
- **No dead code:** checks that can't fire, unused defaults or settings,
  unreachable branches, helpers kept "for compatibility".
- **Don't repeat a condition across branches:** one chain, shared work once
  after it, `else: raise` last.
- **Use the repo's plain idiom,** not a clever one (`math.ceil(a / b)`, not
  `-(-a // b)`); grep how the repo already does it.
- **A type-checker fix narrows types;** it never changes the runtime path.
- **Read data your own code wrote strictly** (`d["key"]`, not a silent `.get()`
  default), with the invariant enforced where it's written.
- **Pass arguments by name;** new parameters go last.

## Names and types
- **One concept, one name,** saying exactly what the value is. Don't reuse a
  variable, or a name that already means something nearby; public names keep
  the project's existing vocabulary.
- **Every magic number is a named constant,** in one block at the top of the
  module, with a one-line comment on what it means or why that value, and a
  link to the spec that defines it.
- **No `Any` when the real type is importable;** units use the repo's aliases
  (`NumBytes`, `KiB`).
- **Return a dataclass or `NamedTuple`, not a bare tuple.**

## Comments and docstrings
- **Every new non-empty file starts with a short header** (module docstring)
  from which a reader could build something similar without reading the code:
  what it's for, how it's built (the key data structures), the design choices
  another implementation might make differently, and anything else the reader
  needs. Same when a change alters what an existing file is for.
- **A comment says why, in one sentence, in one place.** Never restate the code
  or the docstring. Keep existing why-comments.
- **Every factual claim is true of the code.** Grep for text that still
  describes the old behavior, inside and outside the diff.
- **New classes and non-obvious methods get a docstring;** public ones say what
  users see and what isn't covered, not internals.
- **Docstring guidance is an instruction, then numbered steps:** "Override this
  when X. Your implementation should: 1. … 2. …", in its own paragraph after
  the class description.
- **A workaround gets a TODO naming the version that removes it.**

## Performance and memory
- **Size work by its peak memory for every legal input,** not by bytes on disk
  or item counts.
- **Do I/O once, in a few large reads;** count the requests.
- **Copy large data once;** hoist loop-invariant work; skip work the output
  doesn't use.
- **An optimization saves the work it claims:** measure what was done, not only
  the result.
- **Profile code whose cost grows with the input's shape** at the large end.
- **Performance claims state the setup** and are measured on the motivating
  shape, where the cost is, and at the small end.

## Errors and compatibility
- **One bad input fails only its own work.**
- **Don't retry what already retries.** Retry only transient errors; a retry
  neither drops nor duplicates output.
- **Error messages name what failed** and survive pickling.
- **A rewrite accepts every input the old path did and keeps its output,**
  including data earlier versions wrote, or the PR description says what
  changed.
- **Check claims on the oldest supported dependency and CI's pins.** A bug only
  old versions have gets a doc note, not code.
