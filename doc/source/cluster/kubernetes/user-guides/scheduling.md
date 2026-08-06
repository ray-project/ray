---
myst:
  html_meta:
    description: "How Kubernetes and Ray divide scheduling responsibility when you run Ray on Kubernetes, how KubeRay maps container resources to Ray's logical capacity, and which layer to investigate when a workload doesn't start."
---

(kuberay-scheduling)=

# Scheduling on Kubernetes

When you run Ray on Kubernetes, two schedulers are at work. Kubernetes decides which machine each Ray pod runs on. Ray decides which pod each task and actor runs on. Knowing which of the two is holding up your workload is the difference between a quick fix and an afternoon of guessing.

This page explains how the two layers divide the work, how KubeRay connects them, and how to tell which layer to look at when something doesn't start.

## The two layers

The Kubernetes scheduler assigns pods to nodes. It reads the resource requests in each container spec, compares them against allocatable capacity, and applies node selectors, taints, tolerations, affinity rules, and any admission or queueing controller you've installed. When it can't place a pod, the pod stays `Pending`.

The Ray scheduler assigns tasks and actors to Ray nodes. Every Ray node is a Kubernetes pod. Ray reads the logical resource requirements you declare in `@ray.remote`, finds nodes that can satisfy them, and picks one according to the active scheduling strategy. When it can't place a task or actor, that task or actor stays pending while your remaining program logic keeps running.

The two layers stack rather than run side by side. Ray only distributes resources that Kubernetes already granted to the pods in the cluster. A Ray-level request can never conjure capacity the Kubernetes layer hasn't handed over.

The following table summarizes the split:

|  | Kubernetes | Ray |
|---|---|---|
| Unit it places | Pod | Task or actor |
| Target it places onto | Kubernetes node | Ray node, which is a pod |
| What it reads | Container resource requests | Logical resources in `@ray.remote` |
| How you configure it | YAML in the pod template | Python arguments and scheduling strategies |
| Symptom when it's stuck | Pod is `Pending` | Task or actor is pending, pods are running |

## How KubeRay connects the layers

The two layers need to agree on how much capacity each pod has. KubeRay establishes that agreement when it starts each Ray container, deriving Ray's logical resource capacity from the Kubernetes container spec. Three details matter, because this is where the layers most often disagree:

- KubeRay reads the CPU, memory, and GPU **limits** from the Ray container config and uses them as the pod's logical capacity. Starting with KubeRay 1.3.0, it falls back to the CPU request when no CPU limit is set.
- KubeRay rounds CPU quantities up to the nearest integer. A container limit of `500m` becomes one logical CPU to Ray.
- KubeRay ignores memory and GPU **requests**. Set those requests equal to their limits so both layers see the same numbers.

Override any of these with `rayStartParams`. See {ref}`rayStartParams` for the full list. The common override is `num-cpus: "0"` on the head group, which tells Ray the head pod has no CPU capacity and so keeps CPU-requiring workloads off it. The head pod still has real CPU from Kubernetes' point of view. You're only changing what Ray believes it can schedule there.

Check this seam first when the layers seem to disagree. A pod that Kubernetes considers healthy and well-provisioned can still look full, or empty, to Ray.

## Deciding which layer to investigate

Start from what the pods are doing. The pod state separates the cases cleanly.

**Pods running, tasks or actors pending**: The Ray layer is holding the work. Ray classifies each node as *feasible* if it has the required resources at all, and *infeasible* if it doesn't. A GPU task is infeasible on a CPU-only pod no matter how long you wait. Ray further splits feasible nodes into *available* and *unavailable*, depending on whether the resources are free. If every node is infeasible, nothing runs until you add a node type that fits. Check the resource arguments in your `@ray.remote` decorators, any placement group constraints, and the logical capacity KubeRay assigned to your pods. See {ref}`ray-scheduling` for the full model.

**Pods stuck in `Pending`**: The Kubernetes layer is holding the work. Run `kubectl describe pod` and read the scheduler events at the bottom. Typical causes are insufficient allocatable capacity, unsatisfied node selectors or taints, exhausted resource quotas, or a queueing controller holding the workload back. Nothing in your Ray code changes the outcome.

**Pods stuck in `Pending` on an autoscaling cluster**: Both layers are in play, in sequence. The Ray autoscaler decides it needs more Ray nodes and asks KubeRay to create pods. The Kubernetes Cluster Autoscaler then provisions machines so those pods can be placed. A stall can come from either step. See {ref}`ray-k8s-autoscaler-comparison` for how the two autoscalers relate, including why the Ray autoscaler scales on logical resources from your task and actor annotations rather than on observed CPU and memory the way the Horizontal Pod Autoscaler does.

## Choosing where to express a constraint

Express a constraint in Kubernetes when it's about machines: which hardware a pod may land on, which team's quota it draws from, or whether to admit a workload at all. Node selectors, taints, tolerations, and resource quotas are the tools. Ray integrates with five batch schedulers for queueing, priority, and gang scheduling at the pod level. See {ref}`kuberay-kueue`, {ref}`kuberay-kai-scheduler`, {ref}`kuberay-volcano`, {ref}`kuberay-yunikorn`, and {ref}`kuberay-scheduler-plugins`.

Express a constraint in Ray when it's about your application: which tasks need accelerators, which actors must sit together for low-latency communication, and how work spreads across the nodes you already have. Use resource requirements, {ref}`scheduling strategies <ray-scheduling-strategies>`, {ref}`placement groups <ray-placement-group-doc-ref>` for gang scheduling within a Ray cluster, and {ref}`label-based scheduling <kuberay-label-scheduling>` to target nodes by label rather than by ID.

A useful rule of thumb: if the constraint would still make sense with your application code deleted, it belongs in Kubernetes.

## See also

- {ref}`ray-scheduling` for how Ray places tasks and actors, including scheduling strategies and the resource model.
- {ref}`kuberay-config` for the RayCluster configuration fields this page references.
- {ref}`ray-k8s-autoscaler-comparison` for the relationship between the Ray autoscaler and Kubernetes autoscalers.
- {ref}`kuberay-gpu` and {ref}`kuberay-tpu` for accelerator-specific configuration.
