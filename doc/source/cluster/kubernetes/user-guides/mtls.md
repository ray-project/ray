---
myst:
  html_meta:
    description: "Enable automated mutual TLS for RayCluster internal communication with the KubeRay RayClusterMTLS feature gate and cert-manager."
---
(kuberay-mtls)=

# Configuring mTLS for RayClusters

KubeRay v1.7 introduces automated mTLS for RayCluster internal communication through the `RayClusterMTLS` feature gate. When enabled, the operator uses [cert-manager](https://cert-manager.io/) to provision a full public key infrastructure, consisting of a self-signed CA plus head and worker leaf certificates. It also injects the necessary TLS environment variables and volume mounts into every Ray container, so you don't need to manage certificates manually.

This guide covers the automated cert-manager approach. If you prefer to manage certificates yourself, for example with your own CA or init-container scripts, see {ref}`kuberay-tls`.

:::{warning}
`RayClusterMTLS` is an **alpha** feature gate (introduced in KubeRay v1.7, `Default: false`). Enable it explicitly before use. See [Install the KubeRay operator](#install-the-kuberay-operator) below.

Enabling TLS incurs a performance overhead from encryption and decryption of inter-process traffic. The impact is most noticeable in communication-intensive workloads: frequent large object transfers and small tasks with high invocation rates. Compute-bound workloads with minimal data movement see little to no overhead.
:::

## Prerequisites

- KubeRay operator v1.7 or later installed.
- [cert-manager](https://cert-manager.io/docs/installation/) installed in the cluster.
- `kubectl` installed and configured to interact with your cluster.

## Install the KubeRay operator

Install the KubeRay operator by following [Deploy a KubeRay operator](../getting-started/kuberay-operator-installation.md). The minimum version for this guide is v1.7.0. To use this feature, you must enable the `RayClusterMTLS` feature gate. To enable the feature gate when installing the KubeRay operator, run the following command:

```sh
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update

# Install KubeRay operator v1.7.0 with the RayClusterMTLS feature gate enabled
helm install kuberay-operator kuberay/kuberay-operator \
  --version 1.7.0 \
  --set "featureGates[0].name=RayClusterMTLS" \
  --set "featureGates[0].enabled=true"
```

## Enable mTLS on a RayCluster

Set `spec.tlsOptions.enabled: true` in your RayCluster manifest. The RayCluster requires no other TLS configuration. The operator handles the full certificate lifecycle.

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

The head certificate includes the head service fully qualified domain name and head pod IP addresses as Subject Alternative Names (SANs). All worker pods share the worker certificate, which includes worker pod IP addresses as SANs. As the autoscaler creates, deletes, or replaces pods, the operator updates these SANs. Each certificate also always includes `127.0.0.1`.

The operator also injects a `wait-for-tls-ip-san` init container into each Ray pod. The init container blocks startup until cert-manager has added the pod's IP to the correct certificate.

The operator injects the following into every Ray container:

| Environment variable | Value |
|---|---|
| `RAY_USE_TLS` | `1` |
| `RAY_TLS_SERVER_CERT` | `/etc/ray/tls/tls.crt` |
| `RAY_TLS_SERVER_KEY` | `/etc/ray/tls/tls.key` |
| `RAY_TLS_CA_CERT` | `/etc/ray/tls/ca.crt` |

## Verify mTLS is active

Check that cert-manager created the resources and the cluster reached a ready state:

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

cert-manager automatically renews certificates before they expire. Leaf certificates are valid for 90 days and cert-manager begins renewal 15 days before expiry. However, Ray reads TLS material only at process startup. Running Ray processes don't hot-reload updated secrets. If cert-manager renews a certificate while the cluster is running, the pods continue using the original certificate until you restart them.

For most workloads this isn't a concern because RayClusters are typically shorter-lived than the certificate validity period. For long-lived clusters, restart Ray pods after each renewal cycle:

```bash
kubectl delete pod -l ray.io/node-type=head,ray.io/cluster=<cluster-name> -n <namespace>
kubectl delete pods -l ray.io/node-type=worker,ray.io/cluster=<cluster-name> -n <namespace>
```

:::{warning}
Deleting the head pod terminates the Ray head node. Without {ref}`GCS fault tolerance <kuberay-gcs-ft>` enabled, this causes the loss of all cluster state, active jobs, and metadata. Don't run this on a production cluster without understanding the consequences and configuring GCS fault tolerance first.
:::

## Cluster scale limit

The operator adds each worker pod's IP address as a Subject Alternative Name (SAN) in the shared worker certificate. cert-manager encodes each IPv4 address as roughly 6 bytes in Distinguished Encoding Rules (DER) form, which becomes about 8.2 bytes after Privacy Enhanced Mail (PEM) base64 encoding.

cert-manager v1.19 reserves a fixed budget of **30,000 bytes for SANs** within a `maxLeafCertificatePEMSize` of 36,500 bytes, giving a conservative lower bound of approximately **3,658 worker pods per cluster**:

```text
30,000 bytes ÷ 8.2 bytes/IP ≈ 3,658 IPs
```

To raise this limit if your clusters exceed roughly 3,500 workers, use `pemSizeLimitsConfig.maxCertificateSize`, introduced in cert-manager v1.20.

:::{note}
The 3,658 figure is a **lower bound**. The actual limit is higher in most installations because cert-manager allocates more than 30,000 bytes for SANs in practice. See [cert-manager source](https://github.com/cert-manager/cert-manager/blob/ae6723401bd1bef1c00bd3c46a52c15387cd05ba/internal/pem/decode.go#L63-L68) for details.
:::

## Using mTLS with NetworkPolicy

mTLS and the `spec.networkPolicy` network isolation feature are independent. You can combine them to encrypt intra-cluster traffic and restrict which external pods can reach the cluster:

```yaml
spec:
  tlsOptions:
    enabled: true
  networkPolicy:
    mode: DenyAllIngress
```

`spec.networkPolicy` also requires the `RayClusterNetworkPolicy` feature gate on the operator. See the [KubeRay API reference](https://ray-project.github.io/kuberay/reference/api/#networkpolicyconfig) for `NetworkPolicyConfig` field details.
