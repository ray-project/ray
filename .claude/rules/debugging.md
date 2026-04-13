<!-- Debugging tips derived from doc/source/ray-contribute/debugging.rst -->
<!-- Source of truth: docs.ray.io/en/master/ray-contribute/debugging.html -->
- Set RAY_BACKEND_LOG_LEVEL=debug for verbose C++ logs (task execution, object transfer)
- Logs appear in raylet.err in the temp directory
- Start C++ processes in debuggers: RAY_{PROCESS_NAME}_{DEBUGGER}=1 (e.g., RAY_RAYLET_GDB=1)
- GDB requires tmux: RAY_RAYLET_GDB=1 RAY_RAYLET_TMUX=1
- Set RAY_event_stats=1 to dump ASIO event handler stats to debug_state.txt
- Inject RPC latency for deadlock debugging: RAY_testing_asio_delay_us="method=min_us:max_us"
