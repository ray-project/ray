# Common Utilities

This directory contains logic shared across Ray Core and the native libraries.

- All dependencies on non-public APIs in the repo should live here and the public interfaces in the directory should be treated similarly to a "developer API."
- `ray._common` can depend on `ray._private`, but `ray._private` must *not* depend on `ray._common`.
