# Background

Managing dependencies on ray repository is super challenging. Ray maintains a semi-locked monorepo-modeled single dependency set.
Upgrading a single package can lead to many dependency conflicts

Raydepsets is a solution to manage all python dependency sets and lock them for each use case. All platforms, python versions and cuda versions can have separate dependency sets to prevent conflicts

# Usage
Raydepsets is a uv wrapper that allows users to define data driven relationships between requirements, constraints and other dependency sets.
These dependency sets are defined in config files (*.depsets.yaml). Users can generate dependency sets by supplying the config file path (in the ray repo) to the raydepsets build command.

`bazel run //ci/raydepsets:raydepsets -- build ci/raydepsets/example.depsets.yaml`

# Features
The following are the main types of operations that raydepsets handles

## Compile
Standard 'uv pip compile' operation that accepts requirements, constraints and an output path for the lock file

ex.
```yaml
depsets:
  - name: ray_base_test_depset
    operation: compile
    requirements:
      - python/requirements.txt
      - python/requirements/cloud-requirements.txt
      - python/requirements/base-test-requirements.txt
    constraints:
      - /tmp/ray-deps/requirements_compiled.txt
    output: python/deplocks/llm/ray_test_${PYTHON_VERSION}_${CUDA_CODE}.lock
```

## Expand
The expand operation generates a dependency set from other dependency sets
The output depset is a superset of the input depedency set(s)

ex.
```yaml
depsets:
  - name: expanded_ray_depset
    operation: expand
    depsets:
        - ray_base_test_depset
    requirements:
      - python/requirements.txt
    constraints:
      - python/deplocks/llm/ray_test_py311.lock
    output: python/deplocks/llm/rayllm_test_py311.lock
```

## Subset
The subset operation generates a dependency set from another dependency set (source depset), list of requirement files and package names.
The list of requirement files / package names must exist in the source depset.

```yaml
depsets:
  - name: subset_ray_depset
    operation: subset
    source_depset: expanded_ray_depset
    requirements:
      - python/requirements.txt
    output: python/deplocks/llm/subset_ray_llm_py311.lock
```

## Build Arg Sets
Build arg sets are sets of variables that the user can define. Normally used for different python/cuda versions and platforms.
The build arg sets are dictionaries and can be assigned to depsets by name. Build arg sets also allow for variable substitution in depset fields (example below).

ex.
```yaml
build_arg_sets:
  cpu:
    PYTHON_VERSION: py311
    CUDA_CODE: cpu
  cu121:
    PYTHON_VERSION: py311
    CUDA_CODE: cu121
depsets:
  - name: ray_base_test_depset
    operation: compile
    requirements:
      - python/requirements.txt
      - python/requirements/cloud-requirements.txt
      - python/requirements/base-test-requirements.txt
    constraints:
      - /tmp/ray-deps/requirements_compiled.txt
    output: python/deplocks/llm/ray_test_${PYTHON_VERSION}_${CUDA_CODE}.lock
    build_arg_sets:
      - cpu
      - cu121
```
## UV flags
Raydepsets has a set of [default flags](https://github.com/ray-project/ray/blob/master/ci/raydepsets/cli.py#L17) that are applied to every operation.
Users can append or override UV flags with append_args and override_args.

## Append Args
Append args are a list of flags that can be applied to an operation.

```yaml
depsets:
  - name: ray_base_test_depset
    operation: compile
    requirements:
      - python/requirements.txt
      - python/requirements/cloud-requirements.txt
      - python/requirements/base-test-requirements.txt
    constraints:
      - /tmp/ray-deps/requirements_compiled.txt
    output: python/deplocks/llm/ray_test_${PYTHON_VERSION}_${CUDA_CODE}.lock
    append_args:
      - python=3.11
```

## Override Args
Override args are a list of flags that will override all occurances of an existing default flag.

```yaml
depsets:
  - name: ray_base_test_depset
    operation: compile
    requirements:
      - python/requirements.txt
      - python/requirements/cloud-requirements.txt
      - python/requirements/base-test-requirements.txt
    constraints:
      - /tmp/ray-deps/requirements_compiled.txt
    output: python/deplocks/llm/ray_test_${PYTHON_VERSION}_${CUDA_CODE}.lock
    override_args:
      - --python-platform=apple-darwin
```

## Pre Hooks
Pre hooks are executable scripts that are triggered before a depset is generated.
In some cases users may want to modify existing requirement/constraint files before generating a new depset.

```yaml
depsets:
  - name: ray_base_test_depset
    operation: compile
    requirements:
      - python/requirements.txt
      - python/requirements/cloud-requirements.txt
      - python/requirements/base-test-requirements.txt
    constraints:
      - /tmp/ray-deps/requirements_compiled.txt
    output: python/deplocks/llm/ray_test_${PYTHON_VERSION}_${CUDA_CODE}.lock
    pre_hooks:
      - ci/raydepsets/pre_hooks/remove-compiled-headers.sh
```


## Packages
Specific packages and pinned versions can also be defined in a depset.

```yaml
depsets:
  - name: ray_img_depset
    packages:
      - lightning==2.5.4
    constraints:
      - python/requirements_compiled.txt
    output: python/deplocks/ray_img/ray_img.lock
    operation: compile
```
