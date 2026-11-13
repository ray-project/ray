---
paths:
  - "python/ray/data/tests/**/*.py"
---
<!-- Ray Data test rules, distilled from code reviews. Add a rule by PR: general, one line, merged into an existing rule when one covers it. -->
Fewer tests is the default; review tests as hard as code.
- **Each test fails if the change it covers is reverted.**
- **Extend or parametrize an existing test before adding a new one.**
- **Test through the public API** (`ray.data.read_*`, `Dataset` methods).
- **Don't test what other code owns** (Python, pyarrow, mocks that check the
  code calls itself).
- **Asserts must be able to see the bug.**
- **Build test inputs with the real producer,** not by hand.
- **Never drop an existing test's case to make it pass.**
- **A test diff larger than the code diff is a sign to cut.**
- **Test names say what they assert.**

Suggestion: consider property-based tests where fixed cases can't cover the
inputs. Ray doesn't depend on `hypothesis` yet, so propose adding it before
using it.
