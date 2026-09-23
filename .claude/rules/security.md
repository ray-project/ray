<!-- Security rules applying to all code -->
- Add token-based authentication to all new gRPC endpoints and RPC handlers to maintain Ray's security model
- Propagate auth tokens in new API calls (both C++ and Python sides) so that end-to-end authentication remains consistent
- Any new dashboard HTTP endpoint that echoes a `runtime_env` back must redact it for browser-originated requests. `runtime_env={"env_vars": {...}}` is the documented way users pass credentials to jobs, actors and tasks, so these payloads routinely carry cloud keys, DB passwords and API tokens.
- Code that deserializes bytes it does not control in ray data datasources must run under Ray's untrusted-unpickling guard (`ray.util.pickle_guard.forbid_untrusted_unpickling`). Never call `ray.cloudpickle.loads` on such bytes: it is exempt from the guard and reserved for bytes Ray itself produced. A reader whose job is to unpickle exposes an explicit opt-in and wraps only that call in `allow_unsafe_unpickling()`.
