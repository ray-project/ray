(kuberay-network-policy)=

# Configure RayCluster to use NetworkPolicies

The KubeRay operator can generate Kubernetes `NetworkPolicy` resources for each `RayCluster`. When enabled, the operator creates separate policies for the head pod and worker pods, and removes them when the cluster is deleted.

:::{warning}
NetworkPolicy support is alpha and disabled by default. Enable the `RayClusterNetworkPolicy` feature gate before using `spec.networkPolicy`.
:::

## Prerequisites

- KubeRay operator v1.7 or later installed.
- `kubectl` installed and configured to interact with your cluster.

## Install the KubeRay operator

Install the KubeRay operator, following [these instructions](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/kuberay-operator-installation.html). The minimum version for this guide is v1.7.0. To use this feature, you must enable the `RayClusterNetworkPolicy` feature gate. To enable the feature gate when installing the KubeRay operator, run the following command:

```sh
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update

# Install KubeRay operator v1.7.0 with the RayClusterNetworkPolicy feature gate enabled
helm install kuberay-operator kuberay/kuberay-operator \
  --version 1.7.0 \
  --set "featureGates[0].name=RayClusterNetworkPolicy" \
  --set "featureGates[0].enabled=true"
```

## Enable NetworkPolicy

Add a `networkPolicy` field to the `RayCluster` `spec` and choose a mode:

```yaml
apiVersion: ray.io/v1
kind: RayCluster
spec:
  networkPolicy:
    mode: DenyAll
```

Three modes are available:

- **`DenyAll`**: deny all ingress and egress except intra-cluster pod traffic.
- **`DenyAllIngress`**: deny inbound traffic only; outbound is unrestricted.
- **`DenyAllEgress`**: deny outbound traffic only; inbound is unrestricted.

In all modes, the operator always permits pod-to-pod traffic within the same `RayCluster` on all ports. For `DenyAll` and `DenyAllIngress`, the head pod policy also allows the submitter pod to reach the dashboard port when a `RayJob` running in `K8sJobMode` owns the cluster.

## Apply a namespace default-deny policy first

Ray cluster pods and their `NetworkPolicy` resources are created by different controllers asynchronously. A pod can start before its policy is applied, leaving a brief window where all traffic is permitted.

To address this, pre-apply a namespace-level policy scoped to the directions your chosen mode manages. This closes the race window without interfering with the directions your mode leaves unrestricted.

:::{note}
This applies to every pod in the namespace, including pods that KubeRay doesn't manage. To scope it to Ray pods only, add a `podSelector` that matches the label KubeRay applies to head and worker pods:

```yaml
  podSelector:
    matchLabels:
      ray.io/is-ray-node: "yes"
```

Add this selector when you use the `DenyAll` or `DenyAllEgress` variant. The submitter pod for a `RayJob` doesn't carry the `ray.io/is-ray-node` label, so this selector excludes it from the deny rule. Without it, the submitter pod loses egress, and the `RayJob` fails unless you add an extra rule to allow submitter egress.
:::

**`DenyAll`** — deny both ingress and egress:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-all
  namespace: <your-ray-namespace>
spec:
  podSelector: {}
  policyTypes:
  - Ingress
  - Egress
```

**`DenyAllIngress`** — deny inbound only:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-ingress
  namespace: <your-ray-namespace>
spec:
  podSelector: {}
  policyTypes:
  - Ingress
```

**`DenyAllEgress`** — deny outbound only:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-egress
  namespace: <your-ray-namespace>
spec:
  podSelector: {}
  policyTypes:
  - Egress
```

The rules the operator generates are additive on top of whichever policy you apply. Kubernetes NetworkPolicy is allow-only — KubeRay's generated allow rules layer on top without needing to modify the namespace policy.

:::{warning}
Don't apply the `DenyAll` variant with `DenyAllIngress` or `DenyAllEgress` modes. It silently blocks the direction those modes intentionally leave unrestricted, because KubeRay's generated policies can't override a namespace-level deny.
:::

## Required rules under DenyAll and DenyAllEgress

The operator doesn't add Domain Name System (DNS) or API server egress rules. Add both when using `DenyAll` or `DenyAllEgress`.

### DNS egress

Workers reach the head node through its service fully qualified domain name (FQDN). Without a DNS egress rule, workers can't resolve the head address and the cluster won't start. The operator doesn't add this rule by default because DNS deployments vary across clusters. Adjust the selector to match your cluster's DNS provider before applying. Add the rule to both `head.egressRules` and `worker.egressRules`:

```yaml
spec:
  networkPolicy:
    mode: DenyAll
    head:
      egressRules:
      - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kube-system
          podSelector:
            matchLabels:
              k8s-app: kube-dns
        ports:
        - port: 53
          protocol: UDP
        - port: 53
          protocol: TCP
    worker:
      egressRules:
      - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kube-system
          podSelector:
            matchLabels:
              k8s-app: kube-dns
        ports:
        - port: 53
          protocol: UDP
        - port: 53
          protocol: TCP
```

### API server egress

When `enableInTreeAutoscaling: true`, the Ray autoscaler on the head pod must reach the Kubernetes API server to scale the cluster. The operator doesn't add this rule because the correct form depends on the Container Network Interface (CNI) plugin:

| CNI | Required form |
|---|---|
| kindnet | No rule needed — kindnet allows egress to the API server by default |
| Calico, Cilium (with `kube-proxy-replacement`) | `ipBlock` with the endpoint IP (after Destination NAT); a ClusterIP rule blocks autoscaling |
| EKS (Amazon VPC CNI) | `ipBlock` with the `kubernetes` Service ClusterIP; an endpoint IP rule blocks autoscaling |

These results come from testing in [KubeRay #4638](https://github.com/ray-project/kuberay/pull/4638#issuecomment-4660571594). Other CNI configurations may behave differently.

Calico and Cilium also require `policyCIDRMatchMode: Node` set on the CNI (see [this issue](https://github.com/hcloud-k8s/terraform-hcloud-kubernetes/issues/285)) for the endpoint-IP `ipBlock` rule below to match. Without it, the rule may not take effect.

Find the endpoint IP with:

```bash
kubectl get endpointslices -n default -l kubernetes.io/service-name=kubernetes
```

For Calico and Cilium (with `kube-proxy-replacement`), add this rule to `head.egressRules`:

```yaml
- to:
  - ipBlock:
      cidr: <endpoint-ip>/32
  ports:
  - port: 6443
    protocol: TCP
```

For EKS, replace `<endpoint-ip>` with the ClusterIP from `kubectl get svc kubernetes`. As a [suggested alternative for Cilium in KubeRay #4638](https://github.com/ray-project/kuberay/pull/4638#discussion_r3336164401), you can instead use a `CiliumNetworkPolicy` with `toEntities: [kube-apiserver]` to target the API server by identity instead of IP. See Cilium's [Layer 3 policy docs](https://docs.cilium.io/en/stable/security/policy/layer3/) for details.

## RayJob patterns

### RayJob-owned clusters

When a `RayJob` in `K8sJobMode` creates and owns the `RayCluster`, the operator automatically injects an ingress rule allowing the submitter pod to reach the head dashboard port. No additional configuration is needed.

For `HTTPMode` and `SidecarMode`, there is no standalone submitter pod, so no additional ingress rule is required.

### Standalone clusters with clusterSelector

When a `RayJob` targets a pre-existing cluster with `clusterSelector`, the cluster has no `RayJob` owner reference. The operator can't automatically add a submitter ingress rule. To allow the submitter pod to reach the head dashboard port, add an opt-in label to both the `RayCluster` ingress rule and the `RayJob` submitter pod template.

On the `RayCluster`, add an ingress rule under `networkIsolation` that matches the opt-in label:

```yaml
spec:
  networkIsolation:
    mode: DenyAll
    ingressRules:
    - from:
      - podSelector:
          matchLabels:
            ray.io/allow-head-access: "true"
      ports: [ { protocol: TCP, port: 8265 } ]
```

On the `RayJob`, set the same label on the submitter pod via `submitterPodTemplate`:

```yaml
spec:
  submitterPodTemplate:
    metadata:
      labels:
        ray.io/allow-head-access: "true"
```

## Custom rules

Add rules under `head` or `worker` to target that role's policy. The operator appends `head` rules only to the head pod's policy, and `worker` rules only to worker pods' policy.

### Allow Prometheus to scrape metrics

```yaml
spec:
  networkPolicy:
    mode: DenyAll
    head:
      ingressRules:
      - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: <prometheus-namespace>
        ports:
        - port: 8080
          protocol: TCP
```

### Allow worker egress to external services

To allow workers to pull packages or reach external APIs, add an egress rule:

```yaml
spec:
  networkPolicy:
    mode: DenyAll
    worker:
      egressRules:
      - to:
        - ipBlock:
            cidr: 0.0.0.0/0
        ports:
        - port: 443
          protocol: TCP
```

## Full example

See [`ray-cluster.network-policy-deny-all.yaml`](https://raw.githubusercontent.com/ray-project/kuberay/refs/heads/master/ray-operator/config/samples/ray-cluster.network-policy-deny-all.yaml) for a complete annotated example covering `DenyAll` mode, DNS egress, API server egress (commented out with per-CNI notes), Prometheus scraping, and the clusterSelector `RayJob` pattern.
