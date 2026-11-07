---
paths:
  - "python/ray/data/tests/**/*.py"
---
<!-- Ray Data test rules, distilled from code reviews. Add a rule by PR: general, one or two lines, merged into an existing rule when one covers it. -->
Fewer tests is the default; review tests as hard as code.
- **Each fix has a test that fails when only that fix is reverted,** and each
  case fails for a break no other case catches. A case that equals a library
  default, or runs only unchanged code, can't fail.
- **One test per behavior;** an extra assert in an existing test beats a new
  test.
- **Test through the public API** (`ray.data.read_*`, `Dataset` methods), with
  inputs shaped like users' and varying along the dimension the code handles.
- **Don't test what other code owns:** the language, libraries, forwarding
  wrappers, mocks that check the code calls itself.
- **Asserts must be able to see the bug:** no fallback that merges values, no
  assert in a branch that may not run, no expected value computed by the code
  under test.
- **Build inputs with the real producer,** not a hand-written one.
- **Coverage claims reach the new path at the scale that triggers it.**
- **Never drop an existing test's realistic case to make it pass.**
- **A test diff larger than the code diff is a sign to cut.**
- **Test names say what they assert.**
