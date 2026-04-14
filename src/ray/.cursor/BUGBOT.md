# Bugbot Rules

## Rule: RPC Fault Tolerance Standards Guide
- Look at the list of changed files in the PR.
- If any changed file ends in `.proto`, you MUST post the following message:

> ⚠️ This PR modifies one or more \`.proto\` files.
> Please review the RPC fault-tolerance & idempotency standards guide here:
> https://github.com/ray-project/ray/tree/master/doc/source/ray-core/internals/rpc-fault-tolerance.rst

- If no `.proto` files are changed, do not post this message.

## Rule: Clear PR Descriptions and Titles
- Read the PR title and description (the first comment by the author in this PR).
- Post the following message if ANY of these conditions are true:
  - The PR description is blank or contains only template boilerplate
  - The PR title is generic (e.g., "fix bug", "update", "changes") without specifics
  - The PR description does not explain *what* problem is present or *how* the problem is fixed (a sentence for each is sufficient).

> ⚠️ **This PR needs a clearer title and/or description.**
>
> To help reviewers, please ensure your PR includes:
> - **Title**: A concise summary of the change
> - **Description**:
>   - What problem does this solve?
>   - How does this PR solve it?
>   - Any relevant context for reviewers such as:
>      - Why is the problem important to solve?
>      - Why was this approach chosen over others?
>
> See this list of PRs as examples for PRs that have gone above and beyond:
> - https://github.com/ray-project/ray/pull/59613
> - https://github.com/ray-project/ray/pull/57641
> - https://github.com/ray-project/ray/pull/56474
> - https://github.com/ray-project/ray/pull/59610
> - https://github.com/ray-project/ray/pull/52622

## Rule: Use ray_gtest_main for ray_cc_test targets
- Look at changes to ray_cc_test targets in BUILD.bazel files.
- If the ray_cc_test target contains gtest or gtest_main as dependencies, post the following message:

> ⚠️ `gtest` and `gtest_main` are included by default as part of `ray_gtest_main` in `ray_cc_test` targets.
>
> Only include these targets if you need to override the default logic in `ray_gtest_main`. To do so, set `use_ray_gtest_main = False` in the `ray_cc_test` target. You probably don't need to do this.

## Rule: Ray Unit vs. Integration Test Placement
- Look at the list of changed files in the PR.
- If there are any changes under `tests/` directory that has a `unit/` subdirectory (e.g. `python/ray/data/tests/`, `python/ray/serve/tests/`, `python/ray/core/tests/`), apply this rule.

**Unit tests** must live in `tests/unit/` and must **only** test pure Python logic.
Unit tests must not
- Use fixtures starting with `ray_start_`
- Call `ray.*` APIs that depend on ray clusters including `ray.init()`, `ray.put()`, `ray.get()`
- Depend on heavy external resources (e.g., large data downloads, network access).
- Use `time.sleep()`.

1. If a test added to `tests/unit/` violates the unit test rules above, post:

> ⚠️ This test does not appear to be a unit test. Unit tests in `tests/unit/` must only test pure Python logic without depending on a Ray cluster, `time.sleep()`, or external resources. Please move it to the appropriate top-level test file.

2. **Test added to a top-level file under `tests/` that qualifies as a unit test**: If a new test function makes no runtime `ray.*` calls, uses no cluster-starting fixture, and does not use `time.sleep()`, post:

> 💡 This test appears to only test pure Python logic. Consider moving it to `tests/unit/`.
