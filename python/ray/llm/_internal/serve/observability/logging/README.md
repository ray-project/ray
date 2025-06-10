# Ray LLM Serve Logging Configuration

This document describes how to control logging verbosity in Ray LLM Serve to reduce log noise and improve debuggability.

## Overview

Ray LLM Serve provides fine-grained control over different categories of logging to help users:
- Reduce stdout noise during normal operation
- Enable specific logging categories for debugging
- Maintain backwards compatibility with existing deployments

## Environment Variables

### Global Log Level
- **`RAYLLM_LOG_LEVEL`**: Controls the overall log level for Ray LLM components
  - Values: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`
  - Default: `INFO`
  - Example: `export RAYLLM_LOG_LEVEL=WARNING`

### Request Lifecycle Logging
- **`RAYLLM_LOG_REQUEST_LIFECYCLE`**: Controls logging of request start/completion messages
  - Values: `0` (disabled) or `1` (enabled)
  - Default: `1` (enabled)
  - Example: `export RAYLLM_LOG_REQUEST_LIFECYCLE=0`

### Request Failure Logging
- **`RAYLLM_LOG_REQUEST_FAILURES`**: Controls logging of request failures (errors/warnings)
  - Values: `0` (disabled) or `1` (enabled)
  - Default: `1` (enabled)
  - Example: `export RAYLLM_LOG_REQUEST_FAILURES=1`

### Engine Operations Logging
- **`RAYLLM_LOG_ENGINE_OPERATIONS`**: Controls logging of engine startup, shutdown, status changes
  - Values: `0` (disabled) or `1` (enabled)
  - Default: `1` (enabled)
  - Example: `export RAYLLM_LOG_ENGINE_OPERATIONS=0`

### Model Operations Logging
- **`RAYLLM_LOG_MODEL_OPERATIONS`**: Controls logging of model downloading/uploading progress
  - Values: `0` (disabled) or `1` (enabled)
  - Default: `1` (enabled)
  - Example: `export RAYLLM_LOG_MODEL_OPERATIONS=0`

### Streaming Details Logging
- **`RAYLLM_LOG_STREAMING_DETAILS`**: Controls logging of individual streaming request events
  - Values: `0` (disabled) or `1` (enabled)
  - Default: `0` (disabled)
  - Example: `export RAYLLM_LOG_STREAMING_DETAILS=1`

### Prefix Tree Debug Logging
- **`RAYLLM_LOG_PREFIX_TREE_DEBUG`**: Controls debug-level prefix tree operations (very verbose)
  - Values: `0` (disabled) or `1` (enabled)
  - Default: `0` (disabled)
  - Example: `export RAYLLM_LOG_PREFIX_TREE_DEBUG=1`

### Quiet Mode (Convenience)
- **`RAYLLM_QUIET_MODE`**: Enables quiet mode by disabling multiple verbose logging categories
  - Values: `0` (disabled) or `1` (enabled)
  - Default: `0` (disabled)
  - When enabled, automatically sets:
    - `RAYLLM_LOG_REQUEST_LIFECYCLE=0`
    - `RAYLLM_LOG_ENGINE_OPERATIONS=0`
    - `RAYLLM_LOG_MODEL_OPERATIONS=0`
    - `RAYLLM_LOG_STREAMING_DETAILS=0`
  - Example: `export RAYLLM_QUIET_MODE=1`

## Common Use Cases

### Reduce General Verbosity
```bash
# Quiet mode - reduces most operational logs
export RAYLLM_QUIET_MODE=1

# Or manually disable specific categories
export RAYLLM_LOG_REQUEST_LIFECYCLE=0
export RAYLLM_LOG_ENGINE_OPERATIONS=0
```

### Debug Request Issues
```bash
# Enable detailed request logging
export RAYLLM_LOG_REQUEST_LIFECYCLE=1
export RAYLLM_LOG_STREAMING_DETAILS=1
export RAYLLM_LOG_LEVEL=DEBUG
```

### Production Deployment
```bash
# Minimal logging for production
export RAYLLM_LOG_LEVEL=WARNING
export RAYLLM_QUIET_MODE=1
# Keep request failures enabled for debugging
export RAYLLM_LOG_REQUEST_FAILURES=1
```

### Development/Debugging
```bash
# Full verbosity for development
export RAYLLM_LOG_LEVEL=DEBUG
export RAYLLM_LOG_REQUEST_LIFECYCLE=1
export RAYLLM_LOG_ENGINE_OPERATIONS=1
export RAYLLM_LOG_STREAMING_DETAILS=1
```

## Backwards Compatibility

All existing environment variables continue to work:
- `RAYLLM_ENABLE_REQUEST_PROMPT_LOGS`: Still controls request prompt content logging
- `RAYLLM_ENABLE_VERBOSE_TELEMETRY`: Still controls telemetry setup verbosity

By default, all logging categories are enabled to maintain backwards compatibility with existing deployments.

## Implementation Details

The logging configuration is centralized in `ray.llm._internal.serve.observability.logging.config` and applied consistently across all Ray LLM components including:
- Request middleware
- VLLM engine operations
- Model downloading/uploading
- Streaming request handling
