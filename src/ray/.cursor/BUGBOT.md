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
