# Common Utilities Shared Across the Libraries

This directory contains logic shared across Ray Core and the native libraries.

- All dependencies by the libraries on non-public APIs in the repo should live here.
- Interfaces exposed in this directory should be treated similarly to a "developer API."
- `ray._common` can depend on `ray._private`, but `ray._private` must *not* depend on `ray._common`.
- External users and libraries should not depend on code in `ray._common` (the same as `ray._private`).
