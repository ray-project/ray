# Review instructions: logical planning / query optimization

Checks for changes under `python/ray/data/_internal/logical/` (logical
operators, optimizer rules), in addition to `python/ray/data/REVIEW.md`.

## What Important means here

Important findings are semantic ones: an optimizer rule that changes query
results, drops an operator's options, or mis-orders operators.

Readability and correctness of the optimizer rules are highly important.

## Always check

- Optimizer rules preserve semantics: same rows, same columns, and same
  ordering wherever ordering is guaranteed. Pushdown/reorder rules
  (projection, filter, limit) must not move an operator past one it does not
  commute with (e.g. a filter past a map that creates the filtered column, or
  a limit past an operator that changes row count).
- Rules are idempotent and terminate: re-applying the rule to its own output
  is a no-op, and rule ordering assumptions are stated where they exist.
- A new logical operator is wired end to end: planner translation exists
  (`_internal/planner/`), and it round-trips operator fusion / equality checks
  used by the optimizer.
- Operator constructors copy or treat as immutable any user-provided options;
  rules must not mutate shared operator state in place.
- New rules come with tests asserting both the rewritten plan shape and the
  result equivalence on data.
