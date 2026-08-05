(kuberay-network-policy)=

# Configure RayCluster to use NetworkPolicies

The KubeRay operator can generate Kubernetes `NetworkPolicy` resources for each `RayCluster`. When enabled, the operator creates separate policies for the head pod and worker pods, and removes them when the cluster is deleted.

:::{warning}
NetworkPolicy support is alpha and disabled by default. Enable the `RayClusterNetworkPolicy` feature gate before using `spec.networkPolicy`.
:::

## Enable the feature gate

### Helm

Pass a complete `featureGates` override inline at install time in order to enable the feature:

```bash
helm install kuberay-operator kuberay/kuberay-operator -n ray-system -f - <<'EOF'
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
  enabled: false
- name: RayClusterNetworkPolicy
  enabled: true
EOF
```

For an existing installation, export the current values, set `RayClusterNetworkPolicy` to `true`, and upgrade:

```bash
helm get values kuberay-operator -n ray-system -o yaml > values-override.yaml
# edit values-override.yaml: set RayClusterNetworkPolicy enabled: true
helm upgrade kuberay-operator kuberay/kuberay-operator -n ray-system -f values-override.yaml
```

### Kustomize

Add a strategic merge patch to the operator `Deployment`. Because Kubernetes strategic merge patch replaces the `args` list rather than appending to it, include every feature gate you want active, not only the new one:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kuberay-operator
spec:
  template:
    spec:
      containers:
      - name: kuberay-operator
        args:
        - --feature-gates=RayClusterStatusConditions=true,RayJobDeletionPolicy=true,RayMultiHostIndexing=true,RayServiceIncrementalUpgrade=false,RayCronJob=false,RayClusterMTLS=false,RayClusterNetworkPolicy=true
```

Adjust the list to match the gates your installation currently passes. If you're unsure, run `kubectl get deployment kuberay-operator -n ray-system -o jsonpath='{.spec.template.spec.containers[0].args}'` to inspect the live args before patching.

## Enable NetworkPolicy

Add a `networkPolicy` field to `spec` and choose a mode:

```yaml
spec:
  networkPolicy:
    mode: DenyAll
```

Three modes are available:

- **`DenyAll`**: deny all ingress and egress except intra-cluster pod traffic.
```yaml
Name:         raycluster-deny-all-head
Namespace:    default
Created on:   2026-08-05 15:30:02 +0100 IST
Labels:       app.kubernetes.io/created-by=kuberay-operator
              app.kubernetes.io/name=kuberay
              ray.io/cluster=raycluster-deny-all
              ray.io/group=headgroup
Annotations:  <none>
Spec:
  PodSelector:     ray.io/cluster=raycluster-deny-all,ray.io/node-type=head
  Allowing ingress traffic:
    To Port: <any> (traffic allowed to all ports)
    From:
      PodSelector: ray.io/cluster=raycluster-deny-all
  Allowing egress traffic:
    To Port: <any> (traffic allowed to all ports)
    To:
      PodSelector: ray.io/cluster=raycluster-deny-all
    ----------
    To Port: 53/UDP
    To Port: 53/TCP
    To:
      NamespaceSelector: kubernetes.io/metadata.name=kube-system
      PodSelector: k8s-app=kube-dns
  Policy Types: Ingress, Egress
```
- **`DenyAllIngress`**: deny inbound traffic only; outbound is unrestricted.
```yaml
Name:         raycluster-deny-all-ingress-head
Namespace:    default
Created on:   2026-08-05 15:46:24 +0100 IST
Labels:       app.kubernetes.io/created-by=kuberay-operator
              app.kubernetes.io/name=kuberay
              ray.io/cluster=raycluster-deny-all-ingress
              ray.io/group=headgroup
Annotations:  <none>
Spec:
  PodSelector:     ray.io/cluster=raycluster-deny-all-ingress,ray.io/node-type=head
  Allowing ingress traffic:
    To Port: <any> (traffic allowed to all ports)
    From:
      PodSelector: ray.io/cluster=raycluster-deny-all-ingress
  Not affecting egress traffic
  Policy Types: Ingress

Name:         raycluster-deny-all-ingress-workers-workergroup
Namespace:    default
Created on:   2026-08-05 15:46:24 +0100 IST
Labels:       app.kubernetes.io/created-by=kuberay-operator
              app.kubernetes.io/name=kuberay
              ray.io/cluster=raycluster-deny-all-ingress
              ray.io/group=workergroup
Annotations:  <none>
Spec:
  PodSelector:     ray.io/cluster=raycluster-deny-all-ingress,ray.io/group=workergroup,ray.io/node-type=worker
  Allowing ingress traffic:
    To Port: <any> (traffic allowed to all ports)
    From:
      PodSelector: ray.io/cluster=raycluster-deny-all-ingress
  Not affecting egress traffic
  Policy Types: Ingress
```
- **`DenyAllEgress`**: deny outbound traffic only; inbound is unrestricted.
```yaml
Name:         raycluster-deny-all-egress-head
Namespace:    default
Created on:   2026-08-05 15:47:30 +0100 IST
Labels:       app.kubernetes.io/created-by=kuberay-operator
              app.kubernetes.io/name=kuberay
              ray.io/cluster=raycluster-deny-all-egress
              ray.io/group=headgroup
Annotations:  <none>
Spec:
  PodSelector:     ray.io/cluster=raycluster-deny-all-egress,ray.io/node-type=head
  Not affecting ingress traffic
  Allowing egress traffic:
    To Port: <any> (traffic allowed to all ports)
    To:
      PodSelector: ray.io/cluster=raycluster-deny-all-egress
    ----------
    To Port: 53/UDP
    To Port: 53/TCP
    To:
      NamespaceSelector: kubernetes.io/metadata.name=kube-system
      PodSelector: k8s-app=kube-dns
  Policy Types: Egress

Name:         raycluster-deny-all-egress-workers-workergroup
Namespace:    default
Created on:   2026-08-05 15:47:30 +0100 IST
Labels:       app.kubernetes.io/created-by=kuberay-operator
              app.kubernetes.io/name=kuberay
              ray.io/cluster=raycluster-deny-all-egress
              ray.io/group=workergroup
Annotations:  <none>
Spec:
  PodSelector:     ray.io/cluster=raycluster-deny-all-egress,ray.io/group=workergroup,ray.io/node-type=worker
  Not affecting ingress traffic
  Allowing egress traffic:
    To Port: <any> (traffic allowed to all ports)
    To:
      PodSelector: ray.io/cluster=raycluster-deny-all-egress
    ----------
    To Port: 53/UDP
    To Port: 53/TCP
    To:
      NamespaceSelector: kubernetes.io/metadata.name=kube-system
      PodSelector: k8s-app=kube-dns
  Policy Types: Egress
```

In all modes, the operator always permits pod-to-pod traffic within the same `RayCluster` on all ports. For `DenyAll` and `DenyAllIngress`, the head pod policy also allows the submitter pod to reach the dashboard port when a `RayJob` running in `K8sJobMode` owns the cluster.

## Apply a namespace default-deny policy first

Ray cluster pods and their `NetworkPolicy` resources are created by different controllers asynchronously. A pod can start before its policy is applied, leaving a brief window where all traffic is permitted.

To address this, pre-apply a namespace-level policy scoped to the directions your chosen mode manages. This closes the race window without interfering with the directions your mode leaves unrestricted.

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
Don't apply the `DenyAll` variant with `DenyAllIngress` or `DenyAllEgress` modes. It will silently block the direction those modes intentionally leave unrestricted, because KubeRay's generated policies can't override a namespace-level deny.
:::

## Required rules under DenyAll and DenyAllEgress

The operator doesn't add DNS or API server egress rules. Add both when using `DenyAll` or `DenyAllEgress`.

### DNS egress

Workers reach the head node through its service FQDN. Without a DNS egress rule, workers can't resolve the head address and the cluster won't start. Add this rule to both `head.egressRules` and `worker.egressRules`:

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

When `enableInTreeAutoscaling: true`, the Ray autoscaler on the head pod must reach the Kubernetes API server to scale the cluster. The operator doesn't add this rule because the correct form depends on the CNI:

| CNI | Required form |
|---|---|
| kindnet, Calico, Antrea | `ipBlock` with the endpoint IP (post-DNAT) |
| Cilium | `CiliumNetworkPolicy` with `toEntities: [kube-apiserver]`; standard `ipBlock` doesn't work |
| EKS (Amazon VPC CNI) | `ipBlock` with the `kubernetes` Service ClusterIP |

Find the endpoint IP with:

```bash
kubectl get endpointslices -n default -l kubernetes.io/service-name=kubernetes
```

For kindnet, Calico, and Antrea, add this rule to `head.egressRules`:

```yaml
- to:
  - ipBlock:
      cidr: <endpoint-ip>/32
  ports:
  - port: 6443
    protocol: TCP
```

For EKS, replace `<endpoint-ip>` with the ClusterIP from `kubectl get svc kubernetes`. For Cilium, create a `CiliumNetworkPolicy` instead.

## RayJob patterns

### RayJob-owned clusters

When a `RayJob` in `K8sJobMode` creates and owns the `RayCluster`, the operator automatically injects an ingress rule allowing the submitter pod to reach the head dashboard port. No additional configuration is needed.

For `HTTPMode` and `SidecarMode`, there is no standalone submitter pod, so no additional ingress rule is required.

### Standalone clusters with clusterSelector

When a `RayJob` targets a pre-existing cluster with `clusterSelector`, the cluster has no `RayJob` owner reference. The operator can't automatically add a submitter ingress rule. In `K8sJobMode`, KubeRay stamps the submitter pod with stable identity labels, so you can match them directly:

```yaml
spec:
  networkPolicy:
    mode: DenyAll
    head:
      ingressRules:
      - from:
        - podSelector:
            matchLabels:
              ray.io/originated-from-cr-name: <rayjob-name>
              ray.io/originated-from-crd: RayJob
        ports:
        - port: 8265
          protocol: TCP
```

Replace `<rayjob-name>` with the name of the submitting `RayJob`.

## Custom rules

Add per-role rules under `head` and `worker`. The operator appends them to the base policy for that role and doesn't apply head rules to workers or vice versa.

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
