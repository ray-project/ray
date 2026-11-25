# Review instructions: public datasource API

Checks for changes under `python/ray/data/datasource/` (the public
`Datasource`/`Datasink` interfaces and helpers users subclass), in addition
to `python/ray/data/REVIEW.md`.

## What Important means here

This directory is a public extension surface: anything that breaks existing
user subclasses of `Datasource`, `Datasink`, `FileBasedDatasource`, or the
partitioning/metadata helpers is Important.

## Always check

- Interface changes are backward compatible for subclasses: new methods have
  default implementations, new parameters have defaults, and abstract-method
  signatures don't change without a deprecation cycle.
- Every public class/method carries the right stability annotation
  (`@PublicAPI`, `@DeveloperAPI`, `@Deprecated`) and a docstring; moved or
  removed APIs add a `doc/redirects/current.yaml` entry.
- Behavior changes here are reflected in the implementation trees that use
  them. `_internal/datasource_v2/` is the current architecture;
  `_internal/datasource/` (v1) is deprecated and in maintenance mode, so new
  capability on this surface should be driven by v2's needs, not v1's.
- Defaults stay safe: retry/recoverability defaults, `num_rows_per_write`,
  and filename providers must not change semantics for existing user code.
