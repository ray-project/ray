# Bugbot Configuration

## Rule: .proto file reminder
- Look at the list of changed files in the PR.
- If any changed file ends in `.proto`, you MUST post the following message:

> ⚠️ This PR modifies one or more \`.proto\` files.
> Please review the RPC fault-tolerance & idempotency standards here:
> https://github.com/ray-project

- If no `.proto` files are changed, do not post this message.
