(kuberay-mtls)=

# Configuring mTLS for RayClusters

KubeRay v1.7 introduces automated mutual TLS (mTLS) for RayCluster internal communication via the `RayClusterMTLS` feature gate. When enabled, the operator uses [cert-manager](https://cert-manager.io/) to provision a full PKI (self-signed CA, head and worker leaf certificates) and injects the necessary TLS environment variables and volume mounts into every Ray container. All inter-process communication (GCS, raylet, and dashboard) is encrypted and mutually authenticated without any manual certificate management.

This guide covers the automated cert-manager approach. If you prefer to manage certificates yourself (for example, with your own CA or init-container scripts), see {ref}`kuberay-tls`.

:::{warning}
`RayClusterMTLS` is an **alpha** feature gate (introduced in KubeRay v1.7, `Default: false`). Enable it explicitly before use. See [Enable the feature gate](#enable-the-feature-gate) below.

Enabling TLS incurs a performance overhead from encryption and decryption of inter-process traffic. The impact is most noticeable in communication-intensive workloads (frequent large object transfers, small tasks with high invocation rates). Compute-bound workloads with minimal data movement see little to no overhead.
:::

## Prerequisites

- KubeRay operator v1.7 or later installed
- [cert-manager](https://cert-manager.io/docs/installation/) installed in the cluster
- `kubectl` access to the cluster

The Helm examples in this guide install the operator into the `ray-system` namespace. Adjust the namespace in commands if your installation uses a different one.

Install cert-manager if not already present:

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml
# Wait for cert-manager pods to be ready
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/instance=cert-manager \
  -n cert-manager --timeout=90s
```

## Enable the feature gate

`RayClusterMTLS` is disabled by default. You must enable it on the KubeRay operator before creating RayClusters with `spec.tlsOptions.enabled: true`.

### Helm

The `featureGates` array in `values.yaml` is replaced wholesale on `helm upgrade`. The safest approach is to supply a complete override with `RayClusterMTLS` set to `true`.

**Fresh install:**

```bash
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update

helm install kuberay-operator kuberay/kuberay-operator -n ray-system --create-namespace -f - <<'EOF'
featureGates:
- name: RayClusterStatusConditions
  enabled: true
- name: RayJobDeletionPolicy
  enabled: true
- name: RayMultiHostIndexing
  enabled: true
- name: RayServiceIncrementalUpgrade
  enabled: false
- name: RayCronJob
  enabled: false
- name: RayClusterMTLS
  enabled: true
EOF
```

**Upgrading an existing installation:**

```bash
# Export current values, edit, then upgrade
helm get values kuberay-operator -n ray-system -o yaml > current-values.yaml
# Edit current-values.yaml: set enabled: true on the RayClusterMTLS entry in featureGates
helm upgrade kuberay-operator kuberay/kuberay-operator -n ray-system -f current-values.yaml
```

### Kustomize

Create a strategic merge patch for the operator `Deployment`. Because Kubernetes strategic merge patch replaces the `args` list rather than appending to it, include every feature gate you want active, not only the new one:

```yaml
# patch-feature-gates.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kuberay-operator
  namespace: ray-system
spec:
  template:
    spec:
      containers:
      - name: kuberay-operator
        args:
        - --feature-gates=RayClusterStatusConditions=true,RayJobDeletionPolicy=true,RayMultiHostIndexing=true,RayServiceIncrementalUpgrade=false,RayCronJob=false,RayClusterNetworkPolicy=false,RayClusterMTLS=true
```

Adjust the list to match the gates your installation currently passes. If you're unsure, run `kubectl get deployment kuberay-operator -n ray-system -o jsonpath='{.spec.template.spec.containers[0].args}'` to inspect the live args before patching.

Add the patch to your `kustomization.yaml`:

```yaml
namespace: ray-system
resources:
- github.com/ray-project/kuberay/ray-operator/config/default?ref=v1.7.0
patches:
- path: patch-feature-gates.yaml
```

Then apply:

```bash
kubectl apply -k .
```

## Enable mTLS on a RayCluster

Set `spec.tlsOptions.enabled: true` in your RayCluster manifest. No other TLS configuration is required on the RayCluster. The operator handles the full certificate lifecycle.

```yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: raycluster-mtls
spec:
  rayVersion: '2.55.1'
  tlsOptions:
    enabled: true
  headGroupSpec:
    rayStartParams:
      dashboard-host: "0.0.0.0"
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:2.55.1
          resources:
            limits:
              cpu: "1"
              memory: "4Gi"
            requests:
              cpu: "500m"
              memory: "2Gi"
  workerGroupSpecs:
  - replicas: 1
    minReplicas: 1
    maxReplicas: 4
    groupName: small-group
    rayStartParams:
      num-cpus: "1"
    template:
      spec:
        containers:
        - name: ray-worker
          image: rayproject/ray:2.55.1
          resources:
            limits:
              cpu: "1"
              memory: "1Gi"
            requests:
              cpu: "500m"
              memory: "1Gi"
```

Or apply this upstream sample directly:

```bash
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-cluster.mtls.yaml
```

## What the operator creates

When `spec.tlsOptions.enabled: true`, the operator reconciles the following cert-manager resources in the RayCluster's namespace:

| Resource | Name pattern | Purpose |
|---|---|---|
| `Issuer` | `ray-selfsigned-issuer-<cluster>` | Self-signed bootstrap issuer |
| `Certificate` | `ray-ca-certificate-<cluster>` | Self-signed CA certificate |
| `Issuer` | `ray-ca-issuer-<cluster>` | CA-backed issuer for leaf certs |
| `Certificate` | `ray-head-cert-<cluster>` | Head pod leaf certificate |
| `Certificate` | `ray-worker-cert-<cluster>` | Shared worker leaf certificate |

The head certificate includes the head service FQDN and head pod IP addresses as Subject Alternative Names (SANs). All worker pods share the worker certificate, which includes worker pod IP addresses as SANs. The operator updates these SANs as pods are created, deleted, or replaced during autoscaling. Each certificate also always includes `127.0.0.1`.

The operator also injects a `wait-for-tls-ip-san` init container into each Ray pod. The init container blocks startup until cert-manager has added the pod's IP to the correct certificate.

The operator injects the following into every Ray container:

| Environment variable | Value |
|---|---|
| `RAY_USE_TLS` | `1` |
| `RAY_TLS_SERVER_CERT` | `/etc/ray/tls/tls.crt` |
| `RAY_TLS_SERVER_KEY` | `/etc/ray/tls/tls.key` |
| `RAY_TLS_CA_CERT` | `/etc/ray/tls/ca.crt` |

## Verify mTLS is active

Check that cert-manager resources were created and the cluster reached a ready state:

```bash
# Operator event confirming PKI is ready
kubectl get events -n <namespace> --field-selector reason=MTLSPKIReady

# Inspect the issued certificates
kubectl get certificates -n <namespace>
kubectl describe certificate ray-head-cert-raycluster-mtls -n <namespace>

# Confirm TLS env vars are present in a Ray pod
kubectl exec -it <ray-head-pod> -n <namespace> -- env | grep RAY_TLS
```

## Certificate renewal

cert-manager automatically renews certificates before they expire. Leaf certificates are valid for 90 days and cert-manager begins renewal 15 days before expiry. However, **Ray reads TLS material only at process startup**. Running Ray processes do not hot-reload updated secrets. If cert-manager renews a certificate while the cluster is running, the pods continue using the original certificate until they are restarted.

For most workloads this is not a concern because RayClusters are typically shorter-lived than the certificate validity period. For long-lived clusters, restart Ray pods after each renewal cycle:

```bash
kubectl delete pod -l ray.io/node-type=head,ray.io/cluster=<cluster-name> -n <namespace>
kubectl delete pods -l ray.io/node-type=worker,ray.io/cluster=<cluster-name> -n <namespace>
```

:::{note}
Future versions of KubeRay may automate pod restarts on certificate renewal. Track upstream progress in [kuberay#5048](https://github.com/ray-project/kuberay/issues/5048).
:::

## Cluster scale limit

Each worker pod's IP address is added as an IP SAN in the shared worker certificate. cert-manager encodes each IPv4 address as roughly 6 bytes in DER form, which becomes about 8.2 bytes after PEM base64 encoding.

cert-manager v1.19 reserves a fixed budget of **30,000 bytes for SANs** within a `maxLeafCertificatePEMSize` of 36,500 bytes, giving a conservative lower bound of approximately **3,658 worker pods per cluster**:

```
30,000 bytes ÷ 8.2 bytes/IP ≈ 3,658 IPs
```

cert-manager v1.20 introduced `pemSizeLimitsConfig.maxCertificateSize`, which allows raising this limit if your clusters exceed ~3,500 workers.

:::{note}
The 3,658 figure is a **lower bound**. The actual limit is higher in most installations because cert-manager allocates more than 30,000 bytes for SANs in practice. See [cert-manager source](https://github.com/cert-manager/cert-manager/blob/ae6723401bd1bef1c00bd3c46a52c15387cd05ba/internal/pem/decode.go#L63-L68) for details.
:::

## Using mTLS with NetworkPolicy

mTLS and the `spec.networkPolicy` network isolation feature are independent and can be combined. Enabling both encrypts intra-cluster traffic and also restricts which external pods can reach the cluster:

```yaml
spec:
  tlsOptions:
    enabled: true
  networkPolicy:
    mode: DenyAllIngress
```

`spec.networkPolicy` also requires the `RayClusterNetworkPolicy` feature gate on the operator. See the [KubeRay API reference](https://ray-project.github.io/kuberay/reference/api/#networkpolicyconfig) for `NetworkPolicyConfig` field details.
