# Build Tools

Docker image build tooling for Ray Data benchmarks.

## `build_incremental_ray.sh`

Builds Ray docker images with incremental caching for fast iteration.
Supports full builds, base-extra (profiling tools), ray-ml (ML frameworks),
and python-only overlay builds.

### Quick reference

```bash
# Full build (first time or after C++ changes)
./build_incremental_ray.sh --tag v1

# Full build + profiling tools (nsys, perf, gdb, cloud SDKs)
./build_incremental_ray.sh --tag v1-extra --extra

# Full build + profiling tools + ML frameworks
./build_incremental_ray.sh --tag v1-extra-ml --extra --ml

# Python-only overlay (after changing only python/ray/*.py files)
./build_incremental_ray.sh --tag v2 --python-only --base-image <previous-ecr-uri>
```

### How it works

1. Builds a manylinux wheel with a persistent Bazel disk cache
2. Builds `base-deps` (Python dependencies), cached in ECR by lock file hash
3. Builds the `ray` image on top of base-deps
4. (with `--extra`) Builds `base-extra` on top of ray, cached in ECR by
   Dockerfile + lock file hash
5. (with `--ml`) Builds `ray-ml` on top, cached in ECR by requirements hash
6. Pushes the final image to ECR

Must be run from a ray or rayturbo source directory. Run `./build_incremental_ray.sh --help`
for all options.

### Prerequisites

- AWS credentials configured (`aws sts get-caller-identity` must succeed)
- Docker installed and running
- ECR access to `830883877497.dkr.ecr.us-west-2.amazonaws.com/anyscale/ray`
