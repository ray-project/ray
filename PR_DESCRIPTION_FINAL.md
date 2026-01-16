## Why are these changes needed?

This PR replaces SHA-1 with SHA-256 across Ray's internal hash operations to address security concerns in #53435. SHA-1 has known collision vulnerabilities and is deprecated - switching to SHA-256 improves security and FIPS compliance.

## Related issue number

Fixes #53435

## Checks

- [x] I've signed off every commit (using the `-s` flag, i.e., `git commit -s`) in this PR.
- [x] I've run `scripts/format.sh` to lint the changes in this PR.
- [x] I've made sure the tests are passing. Note: there might be a few flaky tests, see the recent failures at https://flakey-tests.ray.io/

## Changes

Updated hash functions in 19 files across runtime environments (pip/conda/uv), autoscaler (config hashing, SSH paths, AWS CloudWatch), and services (Serve, Data, AIR, Tune). Added comprehensive test coverage to verify all hash functions now produce 64-char SHA-256 outputs.

**Note:** This is a breaking change - hash outputs change from 40 to 64 characters, which will invalidate existing runtime environment caches. Performance impact is negligible since hashing isn't on the critical path.

