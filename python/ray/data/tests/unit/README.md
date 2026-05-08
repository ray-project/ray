# Unit Tests

This directory contains unit tests that do not depend on distributed infrastructure or external dependencies.

## Requirements

Unit tests in this directory must be:
- **Fast**: Execute in milliseconds, not seconds
- **Isolated**: No dependencies on Ray runtime, external services, or file I/O
- **Deterministic**: No randomness or time-based behavior

## Restrictions

Tests should NOT:
- Initialize or use the Ray distributed runtime
- Use `time.sleep()` or other time-based delays
- Depend on external services, databases, or file systems
- Make network calls

## Enforcement

The `conftest.py` in this directory enforces these restrictions by preventing:
- `ray.init()` from being called
- `time.sleep()` from being used

If a test requires any of these, it should be moved to the main test directory instead.
