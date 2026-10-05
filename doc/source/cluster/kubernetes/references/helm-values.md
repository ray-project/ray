---
myst:
  html_meta:
    description: "Reference for the KubeRay operator and ray-cluster Helm chart values, including feature gates, default container environment variables, sidecar containers, operator logging, and Argo CD installation."
---

(kuberay-helm-values)=
# Helm chart values

This page explains the KubeRay Helm chart values that need more context than a values table gives you, and points you to the complete list of values for each chart. KubeRay publishes two charts:

- `kuberay-operator` installs the KubeRay operator and its custom resource definitions.
- `ray-cluster` installs a single RayCluster custom resource.

To install the operator, follow the {ref}`kuberay-operator-deploy` guide. To configure the operator's RBAC resources, see {ref}`kuberay-helm-chart-rbac`. To configure a RayCluster, see the {ref}`configuration guide <kuberay-config>`.

## Where to find the complete values

The complete values table for each chart lives in the chart's `README.md` in the `ray-project/kuberay` repository:

- [kuberay-operator chart values](https://github.com/ray-project/kuberay/blob/master/helm-chart/kuberay-operator/README.md#values)
- [ray-cluster chart values](https://github.com/ray-project/kuberay/blob/master/helm-chart/ray-cluster/README.md#values)

Treat those tables as the source of truth. KubeRay generates them from each chart's `values.yaml` with [helm-docs](https://github.com/norwoodj/helm-docs), and a continuous integration check fails if a committed table drifts from its `values.yaml`. The tables list every key, its type, and its default. The sections below cover the values whose behavior the generated table doesn't explain.

## Set chart values

Override a value on the command line with `--set`, or pass a YAML file with `-f`. A file is easier to read and version-control for anything beyond a single flag:

```yaml
# operator-values.yaml
configuration:
  defaultContainerEnvs:
  - name: RAY_enable_open_telemetry
    value: "true"
```

```bash
helm install kuberay-operator kuberay/kuberay-operator --version 1.6.0 -f operator-values.yaml
```

## Operator feature gates

`featureGates` is a list of `{name, enabled}` entries that turn individual operator features on or off. Each entry controls one feature. The default state varies by feature and by chart version, and the generated values table lists each gate's name without describing what it does.

Set a gate through a values file, which keeps the entry readable and stable across releases:

```yaml
# operator-values.yaml
featureGates:
- name: RayServiceIncrementalUpgrade
  enabled: true
```

For the authoritative list of available gates and their defaults in a given chart version, read the [`featureGates` block in the operator's `values.yaml`](https://github.com/ray-project/kuberay/blob/master/helm-chart/kuberay-operator/values.yaml).

## Inject environment variables into Ray containers

`configuration.defaultContainerEnvs` sets environment variables on every Ray container in every RayCluster the operator manages. Use it to apply a Ray feature flag across all Ray pods without editing each RayCluster:

```yaml
# operator-values.yaml
configuration:
  defaultContainerEnvs:
  - name: RAY_enable_open_telemetry
    value: "true"
  - name: RAY_metric_cardinality_level
    value: "recommended"
```

## Add sidecar containers to Ray pods

`configuration.headSidecarContainers` and `configuration.workerSidecarContainers` inject sidecar containers into every Ray head pod and every Ray worker pod the operator manages. A common use is a log-forwarding agent:

```yaml
# operator-values.yaml
configuration:
  headSidecarContainers:
  - name: fluentbit
    image: fluent/fluent-bit:1.9
  workerSidecarContainers:
  - name: fluentbit
    image: fluent/fluent-bit:1.9
```

## Configure operator logging

The `logging` values control how the KubeRay operator writes its own logs.

| Key | Default | Description |
|-----|---------|-------------|
| `logging.stdoutEncoder` | `json` | Encoder for stdout logs. Set to `json` or `console`. |
| `logging.fileEncoder` | `json` | Encoder for file logs. Set to `json` or `console`. |
| `logging.baseDir` | `""` | Directory for the operator log file. |
| `logging.fileName` | `""` | File name for the operator log file. |
| `logging.sizeLimit` | `""` | Size limit for the `emptyDir` volume that holds the operator log file. |

The operator writes to a file only when you set `logging.baseDir` and `logging.fileName`. Use `console` encoding when you read the logs directly and `json` when a log pipeline parses them.

## Install the operator with Argo CD

Argo CD can't apply the operator chart in one step, because the RayCluster, RayJob, RayService, and RayCronJob CRDs are too large for Argo CD to apply as a single resource. Split the installation into two Argo CD applications:

- The first application applies the CRDs from the chart's `crds` directory with the `Replace=true` sync option.
- The second application installs the chart with Helm's `skipCrds=true` option so it doesn't try to apply the CRDs again.

For the full `Application` manifests, see [Working with Argo CD](https://github.com/ray-project/kuberay/blob/master/helm-chart/kuberay-operator/README.md#working-with-argo-cd) in the operator chart README.
