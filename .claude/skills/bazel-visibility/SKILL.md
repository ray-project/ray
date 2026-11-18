---
name: bazel-visibility
description: Rules for setting Bazel target visibility in Ray BUILD files — keep every target as private as possible, grant the minimal visibility needed, and lay out tests in a subdir. Applies when adding or editing any BUILD.bazel target.
---

# Bazel Target Visibility

Ray BUILD targets default to **private**. `ray_cc_library` does **not** add any
default visibility (the old `//visibility:public` default was removed from
`bazel/ray.bzl`). Every target must declare the *minimal* visibility it needs —
nothing wider.

## The rule

A target should be visible to exactly the packages that depend on it, and no
more. In order of preference:

1. **Private (default).** Omit `visibility` entirely if the target is only used
   within its own package. This is the common case — prefer it.

2. **Exact consumer package(s).** If other packages depend on it, list each one
   explicitly:

   ```starlark
   visibility = [
       "//src/ray/gcs:__pkg__",
       "//src/ray/raylet:__pkg__",
   ]
   ```

   `:__pkg__` grants that one package only — not its subpackages. Use it.

3. **Own tests → `:__subpackages__`.** Put a target's tests in a `tests/`
   subdirectory (its own package, e.g. `//src/ray/gcs/tests`). To let that test
   package see the target under test, grant:

   ```starlark
   visibility = [":__subpackages__"],
   ```

   This is the only place `__subpackages__` is expected. A target consumed by a
   *non-test* subpackage still gets that subpackage's exact `//path:__pkg__`.

4. **`//visibility:public` — avoid.** Only for genuine cross-cutting public API,
   and only when a maintainer has agreed. Never reach for it to "make the build
   pass" — add the exact consumer package instead.

## Tests live in a subdir

New `ray_cc_test` targets belong in a `tests/` (or `integration_tests/`)
subpackage, not alongside the library they test. This keeps the library's
production visibility tight (it exposes `:__subpackages__` only to its own tests,
not to arbitrary siblings).

## Workflow for getting visibility right

`bazel build --nobuild` runs loading + analysis (which enforces visibility)
without compiling — fast, and it works on platforms where the full compile
doesn't. Use it as the oracle:

```bash
# Surface every visibility violation across the C++ tree + consumers:
bazel build --nobuild --keep_going \
  //src/ray/... //java/... //cpp/... //python/... //:python/ray/_raylet.so

# Each error names the exact (dependency, consumer) pair:
#   target '//src/ray/util:logging' is not visible from target '//src/ray/common:status'
# Fix by granting the consumer's package (here: //src/ray/common:__pkg__) on the
# dependency — never by widening to public.
```

### Cross-platform caveat (important)

`bazel build --nobuild` only configures targets for the **host** OS. On macOS it
**skips** Linux-only targets (`target_compatible_with` Linux) and the Linux branch
of `select()`s, so it will *not* catch visibility violations on those edges — CI
(Linux) still will. To check every platform's edges at once, use `bazel query`,
which is config-agnostic and over-approximates `select()` (it includes the Linux,
Windows, and default branches regardless of host):

```bash
# Every dependency edge, all platforms — note deps inside select() appear as
# <label value=...> (not <rule-input>), so parse both:
bazel query 'kind("rule", //src/ray/... union //:all)' --output=xml
```

When computing minimal visibility this way, map each `//src/ray` label a consumer
references to that consumer's package. (Package-named targets normalize two ways:
`//a/b/c` == `//a/b/c:c` — treat them as equal.)

Edit visibility programmatically with
[buildozer](https://github.com/bazelbuild/buildtools/tree/master/buildozer):

```bash
buildozer 'add visibility //src/ray/common:__pkg__' //src/ray/util:logging
buildozer 'remove visibility //visibility:public' '//src/...:%ray_cc_library'
```

Then format: `buildifier <changed BUILD files>` (or the `lint` skill).

## When adding a new dependency edge

If package A starts depending on `//B:target`, add `//A:__pkg__` to that
target's `visibility` in B's BUILD file. Keep the list sorted (buildifier does
this). Do not switch the target to `//visibility:public` to shortcut it.
