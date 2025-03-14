# Common Utilities Shared Across the Libraries

This directory contains logic shared across Ray Core and the native libraries.

- All dependencies by the libraries on non-public APIs in the repo should live here. Libraries should _not_ depend on `ray._private`.
- Interfaces exposed in this directory should be treated similarly to a "developer API."
- End users and external libraries not inside the Ray repo should not depend on any code in `ray._common` (the same as `ray._private`).
