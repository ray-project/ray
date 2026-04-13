<!-- Version-controlled. Personal config → CLAUDE.local.md -->
<!-- Library-specific guidance → each library's .claude/CLAUDE.md -->

# Ray

Ray is a unified framework for scaling AI and Python applications.

## Building & Linting

Refer to doc/source/ray-contribute/development.rst for the most up-to date information.
Use the /rebuild skill for guided rebuilds and the /lint skill for linting.

## Testing

See per-library .claude/CLAUDE.md for library-specific test paths.
Default test timeout: 180s (pytest.ini).

## Structure

- src/ray/ — C++ core runtime
- python/ray/ — Python API and AI libraries (data, serve, train, tune)
- rllib/ — RLlib (symlinked from python/ray/rllib)
- doc/source/ — Sphinx documentation

## Development Workflow

- After making C++ or Cython changes, use /rebuild to determine the correct build steps
- Before committing, use /lint to run the appropriate linters
- When investigating CI failures, use /fetch-buildkite-logs to fetch and analyze build logs

## Contributing

See @CONTRIBUTING.rst and @doc/source/ray-contribute/getting-involved.rst
