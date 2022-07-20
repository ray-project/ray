# CI process

_This document is a work-in-progress._
_Please double-check file/function/etc. names for changes, as this document may be out of sync._

### Dependencies

All dependencies (e.g. `apt`, `pip`) should be installed in `install_dependencies()`, following the same pattern as
those that already exist.

Once a dependency is added/removed, please ensure that shell environment variables are persisted appropriately, as CI
systems differ on when `~/.bashrc` et al. are reloaded, if at all. (And they are not necessarily idempotent.)

### Bazel, environment variables, and caching

Any environment variables passed to Bazel actions (e.g. `PATH`) should be idempotent to hit the Bazel cache.

If a different `PATH` gets passed to a Bazel action, Bazel will not hit the cache, and you might trigger a full rebuild
when you really expect an incremental (or no-op) build for an option (say `pip install -e .` after `bazel build //...`).

### Invocation

The CI system (such as Travis) must _source_ (_not_ execute) `ci/ci.sh` and pass the action(s) to execute.
The script either handles the work or dispatches it to other script(s) as it deems appropriate.
This helps ensure any environment setup/teardown is handled appropriately.

### Development best practices & pitfalls (read before adding a new script)

Before adding new scripts, please read this section.

First, please consider modifying an existing script instead (e.g. add your code as a separate function).
Adding new scripts has a number of pitfalls that easily take hours (even days) to track down and fix:

- When _calling_ other scripts (as executables), environment variables (like `PATH`) _cannot_ propagate back up to the
  caller. Often, the caller expects such variables to be updated.

- When _sourcing_ other scripts, global state (`ROOT_DIR`, `main`, `set -e`, etc.) may be overwritten silently, causing
  unexpected behavior.

The following practices can avoid such pitfalls while maintaining intuitive control flow:

- Put all environment-modifying functions in the _top-level_ shell script, so that their invocation behaves intuitively.
  (The sheer length of the script is a secondary concern and can be mitigated by keeping functions modular.)

- Avoid adding new scripts if possible. If it's necessary that you do so, call them instead of sourcing them.
  Note that this implies new scripts should not modify the environment, or the caller will not see such changes!

- Always add code inside a function, not at global scope. Use `local` for variables where it makes sense.
  However, be careful and know the shell rules: for example, e.g. `local x=$(false)` succeeds even under `set -e`.

Ultimately, it's best to _only_ add new scripts if they might need to be executed directly by _non-CI_ code,
as in that case, they should probably not use CI entrypoints (which assume exclusive control over the machine).
