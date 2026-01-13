# CLAUDE.local.md - Direct Ingress Port Context

## Lessons Learned from Verification Session

### 1. When verifying a port/backport between repos:
- **Always identify the exact commits** that are part of the port (use `git log origin/master..HEAD`)
- **Distinguish port commits from upstream merges** - files changed in the diff may come from upstream master merges, not the actual port
- **Don't assume files without equivalents are "new"** - investigate their git history first to see if they came from upstream

### 2. Rayturbo → OSS file mapping:
- Rayturbo has TWO locations for Serve code:
  - `python/ray/anyscale/serve/_private/` - proprietary/extended code
  - `python/ray/serve/_private/` - shared code (same as OSS)
- When porting, files from `anyscale/serve/_private/` go to `serve/_private/` in OSS
- BUILD files: rayturbo uses `python/ray/anyscale/serve/BUILD.bazel`, OSS uses `python/ray/serve/tests/BUILD.bazel`

### 3. File mappings for direct-ingress:
| Rayturbo | OSS |
|----------|-----|
| `anyscale/serve/_private/constants.py` | `serve/_private/constants.py` |
| `anyscale/serve/_private/controller.py` | `serve/_private/controller.py` |
| `anyscale/serve/_private/http_util.py` | `serve/_private/direct_ingress_http_util.py` |
| `anyscale/serve/_private/replica.py` | `serve/_private/replica.py` |
| `anyscale/serve/_private/replica_response_generator.py` | `serve/_private/replica_response_generator.py` |
| `anyscale/serve/BUILD.bazel` | `serve/tests/BUILD.bazel` |
| `anyscale/serve/tests/test_direct_ingress*.py` | `serve/tests/test_direct_ingress*.py` |
| `anyscale/serve/tests/unit/test_controller_direct_ingress.py` | `serve/tests/unit/test_controller_direct_ingress.py` |

### 4. Approved exclusions from the port:
- **HAProxy code**: `haproxy.py`, `haproxy_templates.py`, related tests
- **gRPC direct ingress**: `serialization.py`, `replica_result.py`, `_wrap_grpc_call`
- **Tracing**: `tracing_utils.py` (proprietary Anyscale tracing)

### 5. When generating cursor diff commands:
- Rayturbo path is always on the LEFT (source of truth)
- OSS path is always on the RIGHT (the port to verify)
- Format: `cursor -d <rayturbo_path> <oss_path>`

### 6. To get complete list of files in a port:
```bash
# Get only files from port-specific commits (exclude merge commits)
git log --oneline origin/master..HEAD  # Find port-specific commit hashes
for commit in <commit1> <commit2> ...; do
  git diff-tree --no-commit-id --name-only -r $commit
done | sort -u | grep "^python/ray/serve"
```

### 7. Constant naming convention:
- Rayturbo: `ANYSCALE_*` prefix
- OSS: `RAY_SERVE_*` prefix
