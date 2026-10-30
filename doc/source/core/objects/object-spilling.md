---
myst:
  html_meta:
    description: "Configure where Ray spills objects once the object store fills up, including custom spill directories and spill statistics."
---

# Object spilling
(object-spilling)=

Ray spills objects to a directory in the local filesystem once the object store is full. By default, Ray spills objects to the temporary directory, such as `/tmp/ray/session_2025-03-28_00-05-20_204810_2814690`.

For how spilling works internally, see {ref}`object-spilling-internals`.

(spilling-to-a-custom-directory)=
## Spill to a custom directory

To spill objects to a custom directory, set the `object_spilling_directory` parameter of `ray.init` or the `--object-spilling-directory` option of `ray start`:

::::{tab-set}
:::{tab-item} Python
```{doctest}
ray.init(object_spilling_directory="/path/to/spill/dir")
```
:::

:::{tab-item} CLI
```bash
ray start --object-spilling-directory=/path/to/spill/dir
```
:::
::::

For advanced usage and customizations, contact the [Ray team](https://www.ray.io/community).

## View spill statistics

During spilling, the raylet writes `INFO`-level messages such as the following to its logs, for example, `/tmp/ray/session_latest/logs/raylet.out`:

```
local_object_manager.cc:166: Spilled 50 MiB, 1 objects, write throughput 230 MiB/s
local_object_manager.cc:334: Restored 50 MiB, 1 objects, read throughput 505 MiB/s
```

To view cluster-wide spill statistics, run `ray memory`:

```
--- Aggregate object store stats across all nodes ---
Plasma memory usage 50 MiB, 1 objects, 50.0% full
Spilled 200 MiB, 4 objects, avg write throughput 570 MiB/s
Restored 150 MiB, 3 objects, avg read throughput 1361 MiB/s
```

To display only the cluster-wide spill statistics, run `ray memory --stats-only`.
