# Bugbot Rules

## Rule: RPC Fault Tolerance Standards Guide
- Look at the list of changed files in the PR.
- If any changed file ends in `.proto`, you MUST post the following message:

> ⚠️ This PR modifies one or more \`.proto\` files.
> Please review the RPC fault-tolerance & idempotency standards guide here:
> https://github.com/ray-project/ray/tree/master/doc/source/ray-core/internals/rpc-fault-tolerance.rst

- If no `.proto` files are changed, do not post this message.
