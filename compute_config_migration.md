# Compute Config Schema Migration Guide

## Overview

The release test infrastructure currently uses the **old** Anyscale compute config schema
(`head_node_type` / `worker_node_types`). The **new** schema uses `head_node` / `worker_nodes`
among other changes. See: https://docs.anyscale.com/reference/compute-config-api#compute-config-models

## Schema Differences

| Old Schema                     | New Schema                                    |
|--------------------------------|-----------------------------------------------|
| `head_node_type`               | `head_node`                                   |
| `worker_node_types`            | `worker_nodes`                                |
| `min_workers` / `max_workers`  | `min_nodes` / `max_nodes`                     |
| `use_spot: true/false`         | `market_type: ON_DEMAND/SPOT/PREFER_SPOT`     |
| `cloud_id`                     | `cloud` (name-based) or via `cloud_deployment`|
| `region` (top-level)           | `zones` (list) or via `cloud_deployment`      |

## Python Code That Must Be Updated

### 1. Validation — `release/ray_release/config.py:288-305`

`validate_cluster_compute()` hardcodes old field names:
- `cluster_compute.get("head_node_type", {})` (line ~290)
- `cluster_compute.get("worker_node_types", [])` (line ~296)

New-schema configs would pass validation without being checked.

### 2. Resource Extraction — `release/ray_release/buildkite/concurrency.py:137-147`

`get_test_resources_from_cluster_compute()` uses direct dict access on old keys:
- `cluster_compute["head_node_type"]["instance_type"]`
- `for w in cluster_compute["worker_node_types"]` accessing `w["max_workers"]`, `w["min_workers"]`

New-schema equivalents use `head_node`/`worker_nodes` with `min_nodes`/`max_nodes`.

### 3. KubeRay Conversion — `release/ray_release/kuberay_util.py:1-29`

`convert_cluster_compute_to_kuberay_compute_config()` directly accesses:
- `compute_config["worker_node_types"]`
- `compute_config.get("head_node_type", {}).get("resources", {})`
- Worker fields: `.get("name")`, `.get("min_workers")`, `.get("max_workers")`

## Test Fixtures That Must Be Updated

All contain hardcoded old-schema data structures:

- `release/ray_release/tests/test_config.py:374-424` — `test_validate_cluster_compute_aws_configurations()`
- `release/ray_release/tests/test_buildkite.py:666-699` — `testInstanceResources()`
- `release/ray_release/tests/test_cluster_manager.py:30-39` — `TEST_CLUSTER_COMPUTE` constant
- `release/ray_release/tests/test_kuberay_util.py:10-35` — conversion test fixture
- `release/ray_release/tests/test_glue.py:87` — string representation assertion

## YAML Config Files (~100+)

All compute config YAML files under `release/` use the old schema. Examples:
- `release/autoscaling_tests/aws.yaml`
- `release/air_tests/air_benchmarks/compute_cpu_4_aws.yaml`
- `release/air_tests/air_benchmarks/compute_cpu_4_gce.yaml`
- `release/serve_tests/**/*compute*.yaml`
- `release/train_tests/**/*compute*.yaml`
- `release/llm_tests/**/*compute*.yaml`
- `release/rllib_tests/**/*.yaml`
- And many more across all test suites.

## How Compute Configs Flow Through the System

### Step 1: YAML File Loading — `release/ray_release/template.py:100-107`

`load_test_cluster_compute()` reads the `cluster.cluster_compute` filename from the test
definition (e.g. `aws.yaml`), resolves it relative to the test's `working_dir`, renders
Jinja2 env vars (e.g. `{{env["ANYSCALE_CLOUD_ID"]}}`), and returns a plain dict via
`yaml.safe_load()`.

### Step 2: Validation — `release/ray_release/config.py:279-305`

`validate_test_cluster_compute()` loads the compute config and calls
`validate_cluster_compute()`, which checks AWS EBS `DeleteOnTermination` settings on
`head_node_type` and each entry in `worker_node_types`. (Old-schema keys hardcoded.)

### Step 3: Concurrency/Resource Estimation — `release/ray_release/buildkite/concurrency.py:137-164`

`get_test_resources_from_cluster_compute()` reads `head_node_type.instance_type` and
iterates `worker_node_types` to sum total CPUs/GPUs. This determines which Buildkite
concurrency group the test runs in (e.g. "large-gpu", "small", "medium-gce").

### Step 4: Cluster Manager Setup — `release/ray_release/glue.py:144-221` → `release/ray_release/cluster_manager/cluster_manager.py:59-101`

`_setup_cluster_environment()` loads the compute config, then calls
`cluster_manager.set_cluster_compute()` which:
- Sets `idle_termination_minutes` and `maximum_uptime_minutes` defaults
- Calls `_annotate_cluster_compute()` to inject AWS billing tags into
  `advanced_configurations_json`
- Generates a deterministic `cluster_compute_name` from a hash of the config dict

### Step 5: Anyscale Registration — `release/ray_release/cluster_manager/minimal.py:147-215`

`create_cluster_compute()` searches for an existing compute config by name via
`sdk.search_cluster_computes()`. If not found, calls `sdk.create_cluster_compute()`
passing the raw dict as `config`. The returned `cluster_compute_id` is stored.

### Step 6: Job Submission — `release/ray_release/job_manager/anyscale_job_manager.py:76-87`

`_run_job()` creates a `CreateProductionJob` with
`compute_config_id=self.cluster_manager.cluster_compute_id`. This is where the compute
config actually provisions infrastructure.

### Step 7: KubeRay Path (alternative) — `release/ray_release/glue.py:397-448` → `release/ray_release/kuberay_util.py:1-28`

For KubeRay tests, `convert_cluster_compute_to_kuberay_compute_config()` maps
`worker_node_types` → `worker_nodes` and `head_node_type` → `head_node` (ironically
already producing the new-schema key names for KubeRay's format). Old-schema field
names (`min_workers`, `max_workers`) are read and mapped to `min_nodes`, `max_nodes`.

### Key Observation

The compute config dict is passed **as-is** to `sdk.create_cluster_compute()` at step 5.
The release infrastructure does not transform or reconstruct it. So the Anyscale SDK would
accept new-schema configs if the Python code that reads/validates/inspects the dict
(steps 2, 3, 4, 7) didn't hardcode old-schema field names.

## Old-Schema Fields Found Across YAML Configs

Beyond the core structural fields, these old-schema fields are used in practice:

| Field                              | Scope      | Count | New-Schema Equivalent                     |
|------------------------------------|------------|-------|-------------------------------------------|
| `cloud_id`                         | top-level  | all   | `cloud` (name-based)                      |
| `region`                           | top-level  | all   | `zones` or `cloud_deployment.region`      |
| `max_workers`                      | top-level  | most  | (removed; per-group `max_nodes`)          |
| `head_node_type`                   | top-level  | all   | `head_node`                               |
| `worker_node_types`                | top-level  | all   | `worker_nodes`                            |
| `min_workers` / `max_workers`      | per worker | all   | `min_nodes` / `max_nodes`                 |
| `use_spot`                         | per worker | ~10   | `market_type: ON_DEMAND/SPOT/PREFER_SPOT` |
| `allowed_azs`                      | top-level  | ~73   | `zones`                                   |
| `advanced_configurations_json`     | top-level  | ~87   | `advanced_instance_config`                |
| `gcp_advanced_configurations_json` | top-level  | ~17   | `advanced_instance_config`                |

Note: `aws_advanced_configurations` (per-node) is checked in validation code
(`config.py:291,297`) and tested (`test_config.py`), but **no actual YAML config files**
use this per-node field. All AWS advanced config in practice is top-level
`advanced_configurations_json`. The new schema uses `advanced_instance_config` at all
three levels: top-level on `ComputeConfig`, per `head_node`, and per `worker_nodes[]`.

---

## Migration Plan

### Goal

Make the release test Python infrastructure handle **both** old-schema and new-schema
compute configs simultaneously, so YAML files can be migrated individually over time
without breaking the pipeline.

### Approach: Schema Detection + Accessor Helpers

Introduce a thin abstraction layer that detects which schema a config dict uses and
provides uniform access to the fields that the Python code needs. The config dict itself
is never transformed — it is still passed as-is to the Anyscale SDK.

### Step 1: Add schema detection utility and its tests

**Modified file:** `release/ray_release/exception.py`

Add a custom exception for mixed-schema detection errors, inheriting from
`ReleaseTestConfigError` to match the existing exception hierarchy:

```python
class MixedSchemaError(ReleaseTestConfigError):
    pass
```

**New file:** `release/ray_release/compute_config_utils.py`

Add a function to detect schema version based on the presence of distinguishing keys:

```python
from ray_release.exception import MixedSchemaError


def is_new_schema(cluster_compute: dict) -> bool:
    """Detect whether a compute config dict uses the new Anyscale schema.

    New schema uses 'head_node' / 'worker_nodes'.
    Old schema uses 'head_node_type' / 'worker_node_types'.
    All new-schema fields are optional (the SDK provides defaults), so a
    config with neither old nor new node keys is treated as new-schema.
    Partial presence is accepted — e.g. a config with only 'head_node' but
    no 'worker_nodes' is treated as new-schema.
    Raises MixedSchemaError if both old and new schema keys are present.
    """
    has_new = "head_node" in cluster_compute or "worker_nodes" in cluster_compute
    has_old = (
        "head_node_type" in cluster_compute
        or "worker_node_types" in cluster_compute
    )
    if has_new and has_old:
        raise MixedSchemaError(
            "Compute config contains both old-schema and new-schema keys. "
            "Use only one schema."
        )
    return not has_old
```

Add accessor helpers that abstract over both schemas, returning the data the callers
actually need. Use `Dict`/`List` from `typing` to match the rest of the repository:

```python
from typing import Dict, List


def get_head_node_config(cluster_compute: Dict) -> Dict:
    """Return the head node config sub-dict, regardless of schema version."""
    if is_new_schema(cluster_compute):
        return cluster_compute.get("head_node") or {}
    return cluster_compute.get("head_node_type") or {}


def get_worker_node_configs(cluster_compute: Dict) -> List[Dict]:
    """Return the list of worker node config dicts, regardless of schema version."""
    if is_new_schema(cluster_compute):
        return cluster_compute.get("worker_nodes") or []
    return cluster_compute.get("worker_node_types") or []


def get_worker_min_count(worker: dict, new_schema: bool, default: int = 0) -> int:
    """Return the minimum node/worker count for a worker group dict."""
    if new_schema:
        return worker.get("min_nodes", default)
    return worker.get("min_workers", default)


def get_worker_max_count(worker: dict, new_schema: bool, default: int = 0) -> int:
    """Return the maximum node/worker count for a worker group dict."""
    if new_schema:
        return worker.get("max_nodes", default)
    return worker.get("max_workers", default)
```

Uses `Dict` and `List` from `typing` to match the repository's existing style.

**New file:** `release/ray_release/tests/test_compute_config_utils.py`

Unit tests for the accessor helpers, covering both schemas:

- **`is_new_schema()`** — returns `True` for dicts with `head_node`/`worker_nodes`,
  `True` for dicts with neither old nor new node keys (new schema allows all fields
  to be optional), `False` for dicts with `head_node_type`/`worker_node_types`,
  raises `MixedSchemaError` for dicts containing both old and new schema keys
- **`get_head_node_config()`** — returns the correct sub-dict for each schema, and
  `{}` when the key is missing
- **`get_worker_node_configs()`** — returns the correct list for each schema, and
  `[]` when the key is missing
- **`get_worker_min_count()`** — with `new_schema=True` reads `min_nodes`, with
  `new_schema=False` reads `min_workers`, falls back to default when key is absent
- **`get_worker_max_count()`** — with `new_schema=True` reads `max_nodes`, with
  `new_schema=False` reads `max_workers`, falls back to default when key is absent

### Step 2: Update `validate_cluster_compute()` in `config.py:288-305`

The current code checks:
- `cluster_compute.get("aws", {})` — top-level (old schema, deprecated)
- `head_node_type.get("aws_advanced_configurations", {})` — per-node (old schema)
- `worker_node_types[].get("aws_advanced_configurations", {})` — per-node (old schema)

For the new schema, the equivalent is `advanced_instance_config` at all three levels
(top-level, per head_node, per worker_nodes[]). However, since all new-schema fields
are optional and validated by the SDK, we only need to reject stale old-schema keys
rather than validate `advanced_instance_config` contents.

`is_new_schema()` raises `MixedSchemaError` when both old and new schema keys are
present. Configs with neither old nor new node keys are treated as new-schema
(all new-schema fields are optional). In `validate_cluster_compute()`:
- `MixedSchemaError` — return the error string (config is invalid)
- New schema — reject stale old-schema keys (`aws`, `aws_advanced_configurations`),
  then run `validate_aws_config()` on `advanced_instance_config` at all three levels
  (top-level, head_node, worker_nodes) to enforce the custom EBS
  `DeleteOnTermination` check (the SDK does not enforce this business rule)
- Old schema — validate as before

```python
def validate_cluster_compute(cluster_compute: dict) -> Optional[str]:
    try:
        new_schema = is_new_schema(cluster_compute)
    except MixedSchemaError as e:
        return str(e)

    if new_schema:
        if "aws" in cluster_compute:
            return (
                "'aws' field is invalid in new-schema compute config, "
                "use 'advanced_instance_config' instead"
            )
        head = get_head_node_config(cluster_compute)
        if "aws_advanced_configurations" in head:
            return (
                "'aws_advanced_configurations' is invalid in new-schema "
                "head_node, use 'advanced_instance_config' instead"
            )
        for worker_node in get_worker_node_configs(cluster_compute):
            if "aws_advanced_configurations" in worker_node:
                return (
                    "'aws_advanced_configurations' is invalid in new-schema "
                    "worker_nodes, use 'advanced_instance_config' instead"
                )
        configs_to_check = [
            cluster_compute.get("advanced_instance_config", {}),
            head.get("advanced_instance_config", {}),
        ]
        for worker_node in get_worker_node_configs(cluster_compute):
            configs_to_check.append(
                worker_node.get("advanced_instance_config", {})
            )
    else:
        aws = cluster_compute.get("aws", {})
        head_node_aws = cluster_compute.get("head_node_type", {}).get(
            "aws_advanced_configurations", {}
        )
        configs_to_check = [aws, head_node_aws]
        for worker_node in cluster_compute.get("worker_node_types", []):
            worker_node_aws = worker_node.get("aws_advanced_configurations", {})
            configs_to_check.append(worker_node_aws)

    for config in configs_to_check:
        error = validate_aws_config(config)
        if error:
            return error
    return None
```

### Step 3: Update `concurrency.py`

Fix pre-existing bug in `gcp_gpu_instances` at lines 68-69 where `set` literals
`{64, 8}` and `{96, 8}` are used instead of `tuple` literals. Sets have
non-deterministic unpacking order, so CPU/GPU counts could be swapped:

```python
    "n1-highmem-64-nvidia-tesla-v100-8": (64, 8),
    "n1-highmem-96-nvidia-tesla-v100-8": (96, 8),
```

Update `get_test_resources_from_cluster_compute()` at lines 137-147.
Replace direct dict access with the accessor helpers:

```python
def get_test_resources_from_cluster_compute(cluster_compute: dict) -> Tuple[int, int]:
    instances = []
    new_schema = is_new_schema(cluster_compute)

    head_node = get_head_node_config(cluster_compute)
    if "instance_type" not in head_node:
        raise ValueError("Head node config missing 'instance_type'")
    instances.append((head_node["instance_type"], 1))

    for i, w in enumerate(get_worker_node_configs(cluster_compute)):
        if "instance_type" not in w:
            raise ValueError(
                f"Worker node config at index {i} missing 'instance_type'"
            )
        if new_schema:
            count = w["max_nodes"]
        else:
            count = w.get("max_workers", w.get("min_workers", 1))
        instances.append((w["instance_type"], count))
    # ... rest unchanged (instance type lookup logic is schema-agnostic)
```

### Step 4: Update `_annotate_cluster_compute()` in `cluster_manager.py:83-101`

The tagging code currently writes to `advanced_configurations_json`. For new-schema
configs, it should write to `advanced_instance_config` instead:

```python
def _annotate_cluster_compute(self, cluster_compute, extra_tags):
    if not extra_tags or self.cloud_provider != "aws":
        return cluster_compute

    cluster_compute = cluster_compute.copy()
    if is_new_schema(cluster_compute):
        if "aws" in cluster_compute:
            raise ValueError(
                "aws field is invalid in new-schema compute config, "
                "use advanced_instance_config instead"
            )
        aws = cluster_compute.get("advanced_instance_config", {})
        cluster_compute["advanced_instance_config"] = add_tags_to_aws_config(
            aws, extra_tags, RELEASE_AWS_RESOURCE_TYPES_TO_TRACK_FOR_BILLING
        )
    else:
        if "aws" in cluster_compute:
            raise ValueError(
                "aws field is invalid in old-schema compute config, "
                "use advanced_configurations_json instead"
            )
        aws = cluster_compute.get("advanced_configurations_json", {})
        cluster_compute["advanced_configurations_json"] = add_tags_to_aws_config(
            aws, extra_tags, RELEASE_AWS_RESOURCE_TYPES_TO_TRACK_FOR_BILLING
        )
    return cluster_compute
```

### Step 5: Update `convert_cluster_compute_to_kuberay_compute_config()` in `kuberay_util.py:1-28`

Replace direct old-schema access with the accessor helpers. The new schema's
`worker_nodes` entries do not have a `name` field, so `group_name` needs a fallback:

```python
def convert_cluster_compute_to_kuberay_compute_config(compute_config: dict) -> dict:
    new_schema = is_new_schema(compute_config)
    head_node_resources = get_head_node_config(compute_config).get("resources", {})

    kuberay_worker_nodes = []
    for i, w in enumerate(get_worker_node_configs(compute_config)):
        worker_node_config = {
            "group_name": w.get("name") or f"worker-group-{i}-{w.get('instance_type', 'unknown')}",
            "min_nodes": get_worker_min_count(w, new_schema),
            "max_nodes": get_worker_max_count(w, new_schema),
        }
        if w.get("resources", {}):
            worker_node_config["resources"] = w["resources"]
        kuberay_worker_nodes.append(worker_node_config)

    config = {"head_node": {}, "worker_nodes": kuberay_worker_nodes}
    if head_node_resources:
        config["head_node"]["resources"] = head_node_resources
    return config
```

### Step 6: Update existing test fixtures

Add **new-schema variants** of each existing test fixture alongside the old-schema ones.
Each test that exercises compute config logic should run against both schemas:

- `test_config.py` — restructure `test_validate_cluster_compute_aws_configurations()`
  so that **every** old-schema test dict includes `head_node_type` and
  `worker_node_types` from the start (ensures `is_new_schema()` classifies it as
  old-schema). In particular, the top-level `aws` EBS validation case must not be
  tested with a dict containing only `{"aws": {...}}` — without node keys it would
  be classified as new-schema and trigger key rejection instead of EBS validation.
  Add new-schema test cases using `advanced_instance_config` at top-level, on
  `head_node`, and on `worker_nodes`. Use independent test dicts for new-schema
  cases rather than copying the existing incremental mutation pattern, to avoid
  accidentally mixing old and new schema keys
- `test_buildkite.py` — add new-schema dict to `testInstanceResources()` using
  `head_node`/`worker_nodes` with `min_nodes`/`max_nodes`
- `test_cluster_manager.py` — add `TEST_CLUSTER_COMPUTE_NEW_SCHEMA` constant with
  `head_node`/`worker_nodes`, using `cloud` and `zones` instead of `cloud_id` and
  `region` to match realistic new-schema configs. Add a test for
  `_annotate_cluster_compute` with a new-schema config that verifies tags are
  injected into `advanced_instance_config` (not `advanced_configurations_json`)
- `test_kuberay_util.py` — add new-schema input (without `name` on workers) to conversion
  test, verify `group_name` fallback to `worker-group-{i}`
- `test_glue.py` — parametrize `GlueTest` so `setUp` writes both an old-schema YAML
  string (`head_node_type`/`worker_node_types`) and a new-schema equivalent
  (`head_node`/`worker_nodes`). The new-schema fixture should be:
  `{'head_node': {'instance_type': 'm5a.4xlarge'}, 'worker_nodes': []}`.
  Note: `name` is not a field on `HeadNodeConfig` in the new schema, and
  `worker_nodes: []` means head-only cluster. This ensures the full
  `run_release_test` integration path is exercised for both schemas

### Step 7: Migrate YAML config files (incremental, over time)

Once steps 1-6 are merged, individual YAML config files can be migrated from old to new
schema at any pace. No coordinated big-bang change is required. Each YAML file migration
is a self-contained change.

### Files Changed (summary)

| File                                                     | Change Type  | Status       |
|----------------------------------------------------------|--------------|--------------|
| `release/ray_release/exception.py`                       | Modify       | Done         |
| `release/ray_release/compute_config_utils.py`            | **New file** | Done         |
| `release/ray_release/tests/test_compute_config_utils.py` | **New file** | Done         |
| `release/ray_release/config.py`                          | Modify       | Done         |
| `release/ray_release/buildkite/concurrency.py`           | Modify       | Done         |
| `release/ray_release/cluster_manager/cluster_manager.py` | Modify       | Done         |
| `release/ray_release/kuberay_util.py`                    | Modify       | Done         |
| `release/BUILD.bazel`                                    | Modify       | Done         |
| `release/ray_release/tests/test_config.py`               | Modify       | Done         |
| `release/ray_release/tests/test_buildkite.py`            | Modify       | Done         |
| `release/ray_release/tests/test_cluster_manager.py`      | Modify       | Done         |
| `release/ray_release/tests/test_kuberay_util.py`         | Modify       | Done         |
| `release/ray_release/tests/test_glue.py`                 | Modify       | Done         |

### Bazel Build Changes — `release/BUILD.bazel`

The new `compute_config_utils` module requires Bazel build target registration:

- Added `compute_config_utils` `py_library` target with dependency on `:exception`
- Added `:compute_config_utils` as a dependency to `:kuberay_util` and `:ray_release`
- Excluded `compute_config_utils.py` from the `ray_release` source glob (since it has
  its own target)
- Added `test_compute_config_utils` `py_test` target with dependency on
  `:compute_config_utils`

### Implementation Notes

Steps 1-6 have been implemented. All 98 tests pass (30 new + 68 existing). All 6 Bazel
test targets pass:

- `//release:test_compute_config_utils` (new)
- `//release:test_config`
- `//release:test_buildkite`
- `//release:test_cluster_manager`
- `//release:test_kuberay_util`
- `//release:test_glue`

Minor deviations from the plan during implementation:

- In `validate_cluster_compute()` (Step 2), the stale key rejection and
  `advanced_instance_config` collection for worker nodes were merged into a single loop
  over `get_worker_node_configs()`, rather than iterating twice as shown in the plan
  snippet.
- In `test_glue.py` (Step 6), new-schema coverage was implemented via a
  `GlueTestNewSchema(GlueTest)` subclass that overrides `setUp` to write new-schema
  YAML, inheriting all test methods from the parent class.

### What Does NOT Change

- `release/ray_release/template.py` — loads YAML into a dict; schema-agnostic already.
- `release/ray_release/cluster_manager/minimal.py` — passes the dict as-is to the SDK;
  schema-agnostic already.
- `release/ray_release/job_manager/anyscale_job_manager.py` — uses `cluster_compute_id`
  only; schema-agnostic already.
- `release/ray_release/glue.py` — orchestration calls are schema-agnostic already (it
  just passes the dict through). No changes needed.
