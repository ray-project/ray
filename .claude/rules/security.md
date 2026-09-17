<!-- Security rules applying to all code -->
- Add token-based authentication to all new gRPC endpoints and RPC handlers to maintain Ray's security model
- Propagate auth tokens in new API calls (both C++ and Python sides) so that end-to-end authentication remains consistent
- Any new dashboard HTTP endpoint that echoes a `runtime_env` back must redact it for browser-originated requests. `runtime_env={"env_vars": {...}}` is the documented way users pass credentials to jobs, actors and tasks, so these payloads routinely carry cloud keys, DB passwords and API tokens.
- A new or changed Ray Data datasource that yields a `pa.Table` deserialized from external input (parquet, Arrow IPC, a service or library returning Arrow) without calling `raise_on_pickle_object_columns(table)` before yielding or materializing it is a remote code execution bug and should be avoided.
