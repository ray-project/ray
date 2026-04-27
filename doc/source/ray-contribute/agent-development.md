(agent-development)=

# Using Agents for Development

AI coding agents can accelerate development on the Ray codebase. This guide covers
how the Ray project is configured for agent-assisted development and how to set up
your local environment.

```{contents}
:local:
:backlinks: none
```

(claude-code-setup)=

## Claude Code

[Claude Code](https://code.claude.com) is an AI coding assistant that understands the Ray codebase
through a hierarchy of instruction files, rules, and skills. For installation instructions, see the
[official documentation](https://code.claude.com/docs).

### Project configuration

The Ray repository includes shared Claude Code configuration that is version-controlled:

- `.claude/CLAUDE.md` — root instructions loaded in every session
- `<library>/.claude/CLAUDE.md` — library-specific instructions loaded on-demand
  (e.g., `python/ray/data/.claude/CLAUDE.md`)
- `.claude/rules/` — coding rules scoped by file type
- `.claude/skills/` — reusable workflows (rebuild, lint, fetch CI logs)
- `.claude/agents/` — project-specific subagents

Personal configuration lives in files that are **not** version-controlled:

- `CLAUDE.local.md` — your environment-specific instructions
- `.claude/settings.local.json` — your personal permission overrides

### Personal setup

After installing Claude Code, create a `CLAUDE.local.md` file in the repository root
with your environment-specific configuration:

```markdown
## My Environment
- Python: /path/to/your/python
- Test runner: /path/to/your/python -m pytest

## My Git Setup
- origin = your-username/ray (fork)
- upstream = ray-project/ray (main repo)

## Preferences
- Add any personal preferences here
```

This file is gitignored and will not be committed.

### Cross-worktree setup

If you use multiple git worktrees, `CLAUDE.local.md` only exists in the worktree where
you created it. To automatically symlink it from your main checkout whenever a new
worktree is created, set up a `post-checkout` git hook:

1. From your main Ray checkout (not a worktree), create the hook file at
   `$(git rev-parse --git-common-dir)/hooks/post-checkout` with the following contents:

   ```bash
   #!/bin/bash
   # Auto-symlink CLAUDE.local.md into new worktrees.
   MAIN_REPO="$(git rev-parse --git-common-dir)/.."
   MAIN_LOCAL_MD="$(cd "$MAIN_REPO" && pwd)/CLAUDE.local.md"
   if [ -f "$MAIN_LOCAL_MD" ] && [ ! -e "CLAUDE.local.md" ]; then
       ln -s "$MAIN_LOCAL_MD" CLAUDE.local.md
   fi
   ```

2. Make it executable:

   ```bash
   chmod +x "$(git rev-parse --git-common-dir)/hooks/post-checkout"
   ```

3. The hook fires automatically when you create a new worktree with
   `git worktree add`. For existing worktrees, run the symlink manually:

   ```bash
   ln -s /path/to/ray/CLAUDE.local.md CLAUDE.local.md
   ```

### Buildkite token setup

The `/fetch-buildkite-logs` skill requires a Buildkite API token to fetch CI logs.

1. Go to <https://buildkite.com/user/api-access-tokens>
2. Create a new token with these scopes:

   - `read_builds`
   - `read_build_logs`

3. Add it to your shell profile:

   ```bash
   # Add to ~/.bashrc or ~/.zshrc
   export BUILDKITE_API_TOKEN="your-token-here"
   ```

4. Reload your shell: `source ~/.bashrc`

### Available skills

Shared skills available in every session:

- `/rebuild` — guided Ray rebuild based on what files changed
- `/lint` — run linting and formatting checks
- `/fetch-buildkite-logs` — fetch and analyze Buildkite CI logs

### Adding team rules

Each Ray library has a `.claude/rules/` directory where teams can add coding rules
that apply when working on their files. To add a new rule:

1. Create a `.md` file in your library's rules directory, e.g.,
   `python/ray/data/.claude/rules/data-conventions.md`

2. Add a `paths` frontmatter to scope it to your files:

   ```markdown
   ---
   paths:
     - "python/ray/data/**/*.py"
   ---
   - Use logical operators from ray.data._internal.logical.operators
   - Prefer streaming execution over batch where possible
   ```

Rules without `paths` frontmatter load unconditionally in every session.
See the `README.md` in each rules directory for examples.

### Adding team skills

Skills are reusable workflows that load on-demand when invoked with `/<skill-name>`.
To add a new skill:

1. Create a directory under your library's `.claude/skills/`, e.g.,
   `python/ray/data/.claude/skills/debug-data/`

2. Add a `SKILL.md` file with frontmatter:

   ```markdown
   ---
   name: debug-data
   description: Debug Ray Data pipeline issues
   ---

   # Debug Data Pipeline

   ## Steps
   1. Check the Data execution plan...
   2. Look for common issues...
   ```

Skills in a library's `.claude/skills/` directory are discovered when working in that
library. Shared skills in `.claude/skills/` are available everywhere.
