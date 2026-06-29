<!-- Version-controlled. Personal config → CLAUDE.local.md -->
<!-- Library-specific guidance → each library's .claude/CLAUDE.md -->

@../AGENTS.md

## Claude Code workflow

- After C++ or Cython changes, use the /rebuild skill to determine the build steps.
- After code changes, use the /lint skill to fix lint issues on modified files.
- When investigating CI failures, use the /fetch-buildkite-logs skill.
