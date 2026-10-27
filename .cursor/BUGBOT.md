# Bugbot Rules

## Rule: Redact runtime envs before they reach unauthenticated sinks
- `runtime_env={"env_vars": {...}}` is the documented way users pass credentials to jobs, actors and tasks, so a `runtime_env` (as a dict, a serialized JSON string, a `RuntimeEnvInfo`, or a `RuntimeEnvContext`) routinely carries cloud keys, DB passwords and API tokens.
- Look at C++ and Python changes that write a `runtime_env` or its `env_vars` to any sink that isn't protected by Ray's auth token. These sinks include:
  - Log lines (`RAY_LOG`, `RAY_CHECK`/`RAY_CHECK_*` messages, `logger.*`, `print`), because the dashboard serves log files.
  - Exception, `Status` and error messages, because they end up in logs and in task and actor errors.
  - Process command lines and subprocess arguments, because the reporter agent shows them in the dashboard.
  - Events or payloads exported to external endpoints.
  - Dashboard HTTP responses to browser-originated requests (gate on `should_redact_runtime_env(req)`).
  - Debug strings such as `DebugString()`, `__repr__` and `__str__` that feed any of the above.
- If the value isn't passed through the redaction helpers first, post the following message:

> ⚠️ This change writes a `runtime_env` to a sink that isn't protected by Ray's auth token. Its `env_vars` values may hold user credentials. Redact it first:
> Redact only the copy you print. Leave the original untouched, because Ray launches workers from it.

- Don't post this message for runtime envs sent between Ray components over authenticated gRPC, stored in GCS, or used to launch workers. Those paths must keep the plaintext values.

## Rule: Keep C++ and Python runtime env redaction consistent
- Look at the list of changed files in the PR.
- If either `src/ray/common/runtime_env_redaction.{h,cc}` or `python/ray/_private/runtime_env/redaction.py` is changed and the other isn't, compare their behavior. They must agree on:
  - The placeholder (`kRedactedPlaceholder` / `REDACTED_PLACEHOLDER`).
  - The set of secret fields (`kSecretRuntimeEnvFields` / `SECRET_RUNTIME_ENV_FIELDS`).
  - What is masked: values are replaced, keys are kept.
  - Fail-closed handling: input that is empty, isn't valid JSON, or isn't a JSON object.
  - When the input is returned unchanged instead of re-serialized.
- If the change makes the two diverge, post the following message:

> ⚠️ This PR changes runtime env redaction in only one language. `src/ray/common/runtime_env_redaction.{h,cc}` and `python/ray/_private/runtime_env/redaction.py` must mask the same fields with the same placeholder and fail closed the same way. Otherwise a secret redacted on one side leaks from the other. Please make the matching change on the other side and update both `src/ray/common/tests/runtime_env_redaction_test.cc` and the Python redaction tests.

- If both files are changed, check that they still agree on the points above, and post the same message if they don't.
