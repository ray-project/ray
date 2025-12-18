(deploying-on-argocd-example)=

# Deploying Ray Clusters via ArgoCD

This guide provides a step-by-step approach for deploying Ray clusters on Kubernetes using ArgoCD. ArgoCD is a declarative GitOps tool that enables you to manage Ray cluster configurations in Git repositories with automated synchronization, version control, and rollback capabilities. This approach is particularly valuable when managing multiple Ray clusters across different environments, implementing audit trails and approval workflows, or maintaining infrastructure-as-code practices. For simpler use cases like single-cluster development or quick experimentation, direct kubectl or Helm deployments may be sufficient.
You can read more about the benefits of ArgoCD [here](https://argo-cd.readthedocs.io/en/stable/#why-argo-cd).

This example demonstrates how to deploy the KubeRay operator and a RayCluster with three different worker groups, leveraging ArgoCD's GitOps capabilities for automated cluster management.

## Prerequisites

Before proceeding with this guide, ensure you have the following:

* A Kubernetes cluster with appropriate resources for running Ray workloads.
* `kubectl` configured to access your Kubernetes cluster.
* (Optional)[ArgoCD installed](https://argo-cd.readthedocs.io/en/stable/getting_started/) on your Kubernetes cluster.
* (Optional)[ArgoCD CLI](https://argo-cd.readthedocs.io/en/stable/cli_installation/) installed on your local machine (recommended for easier application management. It might need [port-forwarding and login](https://argo-cd.readthedocs.io/en/stable/getting_started/#port-forwarding) depending on your environment).
* (Optional)Access to the ArgoCD UI or API server.

## Step 1: Deploy KubeRay Operator CRDs

First, deploy the Custom Resource Definitions (CRDs) required by the KubeRay operator. Create a file named `ray-operator-crds.yaml` with the following content:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: ray-operator-crds
  namespace: argocd
spec:
  project: default
  destination:
    server: https://kubernetes.default.svc
    namespace: ray-cluster
  source:
    repoURL: https://github.com/ray-project/kuberay
    targetRevision: v1.5.1  # update this as necessary
    path: helm-chart/kuberay-operator/crds
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
      - Replace=true
```

Apply the ArgoCD Application:

```sh
kubectl apply -f ray-operator-crds.yaml
```

Wait for the CRDs Application to sync and become healthy. You can check the status using:

```sh
kubectl get application ray-operator-crds -n argocd
```

Which should eventually give something like:
```
NAME                SYNC STATUS   HEALTH STATUS
ray-operator-crds   Synced        Healthy
```

Alternatively, if you have the ArgoCD CLI installed, you can wait for the application:

```sh
argocd app wait ray-operator-crds
```

## Step 2: Deploy the KubeRay Operator

After the CRDs are installed, deploy the KubeRay operator itself. Create a file named `ray-operator.yaml` with the following content:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: ray-operator
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/ray-project/kuberay
    targetRevision: v1.5.1  # update this as necessary
    path: helm-chart/kuberay-operator
    helm:
      skipCrds: true   # CRDs are already installed in Step 1
  destination:
    server: https://kubernetes.default.svc
    namespace: ray-cluster
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

Note the `skipCrds: true` setting in the Helm configuration. This is required because the CRDs were installed separately in Step 1.

Apply the ArgoCD Application:

```sh
kubectl apply -f ray-operator.yaml
```

Wait for the operator Application to sync and become healthy. You can check the status using:

```sh
kubectl get application ray-operator -n argocd
```

Which should give the following output eventually:
```
NAME           SYNC STATUS   HEALTH STATUS
ray-operator   Synced        Healthy
```

Alternatively, if you have the ArgoCD CLI installed:

```sh
argocd app wait ray-operator
```

Verify that the KubeRay operator pod is running:

```sh
kubectl get pods -n ray-cluster -l app.kubernetes.io/name=kuberay-operator
```



## Step 3: Deploy a RayCluster

Now deploy a RayCluster with autoscaling enabled and three different worker groups. Create a file named `raycluster.yaml` with the following content:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: raycluster
  namespace: argocd
spec:
  project: default
  destination:
    server: https://kubernetes.default.svc
    namespace: ray-cluster
  ignoreDifferences:
    - group: ray.io
      kind: RayCluster
      name: raycluster-kuberay
      namespace: ray-cluster
      jqPathExpressions:
        - .spec.workerGroupSpecs[].replicas
  source:
    repoURL: https://ray-project.github.io/kuberay-helm/
    chart: ray-cluster
    targetRevision: "1.5.1"
    helm:
      releaseName: raycluster
      valuesObject:
        image:
          repository: docker.io/rayproject/ray
          tag: latest
          pullPolicy: IfNotPresent
        head:
          rayStartParams:
            num-cpus: "0"
          enableInTreeAutoscaling: true
          autoscalerOptions:
            version: v2
            upscalingMode: Default
            idleTimeoutSeconds: 600 # 10 minutes
            env:
            - name: AUTOSCALER_MAX_CONCURRENT_LAUNCHES
              value: "100"
        worker:
          groupName: standard-worker
          replicas: 1
          minReplicas: 1
          maxReplicas: 200
          rayStartParams:
            resources: '"{\"standard-worker\": 1}"'
          resources:
            requests:
              cpu: "1"
              memory: "1G"
        additionalWorkerGroups:
          additional-worker-group1:
            image:
              repository: docker.io/rayproject/ray
              tag: latest
              pullPolicy: IfNotPresent
            disabled: false
            replicas: 1
            minReplicas: 1
            maxReplicas: 30
            rayStartParams:
              resources: '"{\"additional-worker-group1\": 1}"'
            resources:
              requests:
                cpu: "1"
                memory: "1G"
          additional-worker-group2:
            image:
              repository: docker.io/rayproject/ray
              tag: latest
              pullPolicy: IfNotPresent
            disabled: false
            replicas: 1
            minReplicas: 1
            maxReplicas: 200
            rayStartParams:
              resources: '"{\"additional-worker-group2\": 1}"'
            resources:
              requests:
                cpu: "1"
                memory: "1G"
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

Apply the ArgoCD Application:

```sh
kubectl apply -f raycluster.yaml
```

Wait for the RayCluster Application to sync and become healthy. You can check the status using:

```sh
kubectl get application raycluster -n argocd
```

Alternatively, if you have the ArgoCD CLI installed:

```sh
argocd app wait raycluster
```

Verify that the RayCluster is running:

```sh
kubectl get raycluster -n ray-cluster
```

Which will give something like:
```
NAME                 DESIRED WORKERS   AVAILABLE WORKERS   CPUS   MEMORY   GPUS   STATUS   AGE
raycluster-kuberay   3                 3                   ...    ...      ...    ready    ...
```


You should see the head pod and worker pods:

```sh
kubectl get pods -n ray-cluster
```

Gives something like:
```
NAME                                               READY   STATUS    RESTARTS   AGE
kuberay-operator-6c485bc876-28dnl                  1/1     Running   0          11d
raycluster-kuberay-additional-worker-group1-n45rc   1/1     Running   0          5d21h
raycluster-kuberay-additional-worker-group2-b2455   1/1     Running   0          2d18h
raycluster-kuberay-head                            2/2     Running   0          5d21h
raycluster-kuberay-standard-worker-worker-bs8t8    1/1     Running   0          5d21h
```

## Understanding Ray Autoscaling with ArgoCD

### Determining Fields to Ignore

The `ignoreDifferences` section in the RayCluster Application configuration is critical for proper autoscaling. To determine which fields need to be ignored, you can inspect the RayCluster resource to identify fields that change dynamically during runtime.

First, describe the RayCluster resource to see its full specification:

```sh
kubectl describe raycluster raycluster-kuberay -n ray-cluster
```

Or, get the resource in YAML format to see the exact field paths:

```sh
kubectl get raycluster raycluster-kuberay -n ray-cluster -o yaml
```

Look for fields that are modified by controllers or autoscalers. In the case of Ray, the autoscaler modifies the `replicas` field under each worker group spec. You'll see output similar to:

```yaml
spec:
  workerGroupSpecs:
  - replicas: 5  # This value changes dynamically
    minReplicas: 1
    maxReplicas: 200
    groupName: standard-worker
    # ...
```

When ArgoCD detects differences between the desired state (in Git) and the actual state (in the cluster), it will show these in the UI or via CLI:

```sh
argocd app diff raycluster
```

If you see repeated differences in fields that should be managed by controllers (like autoscalers), those are candidates for `ignoreDifferences`.

### Configuring ignoreDifferences

The `ignoreDifferences` section in the RayCluster Application configuration tells ArgoCD which fields to ignore. Without this setting, ArgoCD and the Ray Autoscaler may conflict, resulting in unexpected behavior when requesting workers dynamically (for example, using `ray.autoscaler.sdk.request_resources`). Specifically, when requesting N workers, the Autoscaler might not spin up the expected number of workers because ArgoCD could revert the replica count back to the original value defined in the Application manifest.

The recommended approach is to use `jqPathExpressions`, which automatically handles any number of worker groups:

```yaml
ignoreDifferences:
  - group: ray.io
    kind: RayCluster
    name: raycluster-kuberay
    namespace: ray-cluster
    jqPathExpressions:
      - .spec.workerGroupSpecs[].replicas
```

This configuration tells ArgoCD to ignore differences in the `replicas` field for all worker groups. The `jqPathExpressions` field uses JQ syntax with array wildcards (`[]`), which means you don't need to update the configuration when adding or removing worker groups.

**Note**: The `name` and `namespace` must match your RayCluster resource name and namespace. Verify these values separately if you've customized them.

**Alternative: Using jsonPointers**

If you prefer explicit configuration, you can use `jsonPointers` instead:

```yaml
ignoreDifferences:
  - group: ray.io
    kind: RayCluster
    name: raycluster-kuberay
    namespace: ray-cluster
    jsonPointers:
      - /spec/workerGroupSpecs/0/replicas
      - /spec/workerGroupSpecs/1/replicas
      - /spec/workerGroupSpecs/2/replicas
```

With `jsonPointers`, you must explicitly list each worker group by index:
- `/spec/workerGroupSpecs/0/replicas` - First worker group (the default `worker` group)
- `/spec/workerGroupSpecs/1/replicas` - Second worker group (`additional-worker-group1`)
- `/spec/workerGroupSpecs/2/replicas` - Third worker group (`additional-worker-group2`)

If you add or remove worker groups, you **must** update this list accordingly. The index corresponds to the order of worker groups as they appear in the RayCluster spec, with the default `worker` group at index 0 and `additionalWorkerGroups` following in the order they are defined.

See the [ArgoCD diff customization documentation](https://argo-cd.readthedocs.io/en/stable/user-guide/diffing/) for more details on both approaches.

By ignoring these differences, ArgoCD allows the Ray Autoscaler to dynamically manage worker replicas without interference.

## Step 4: Access the Ray Dashboard

To access the Ray Dashboard, port-forward the head service:

```sh
kubectl port-forward -n ray-cluster svc/raycluster-kuberay-head-svc 8265:8265
```

Navigate to `http://localhost:8265` in your browser to view the Ray Dashboard.

## Customizing the Configuration

You can customize the RayCluster configuration by modifying the `valuesObject` section in the `raycluster.yaml` file:

* **Image**: Change the `repository` and `tag` to use different Ray versions.
* **Worker Groups**: Add or remove worker groups by modifying the `additionalWorkerGroups` section.
* **Autoscaling**: Adjust `minReplicas`, `maxReplicas`, and `idleTimeoutSeconds` to control autoscaling behavior.
* **Resources**: Modify `rayStartParams` to allocate custom resources to worker groups.

After making changes, commit them to your Git repository. ArgoCD will automatically sync the changes to your cluster if automated sync is enabled.

## Alternative: Deploy Everything in One File

If you prefer to deploy all components at once, you can combine all three ArgoCD Applications into a single file. Create a file named `ray-argocd-all.yaml` with the following content:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: ray-operator-crds
  namespace: argocd
spec:
  project: default
  destination:
    server: https://kubernetes.default.svc
    namespace: ray-cluster
  source:
    repoURL: https://github.com/ray-project/kuberay
    targetRevision: v1.5.1  # update this as necessary
    path: helm-chart/kuberay-operator/crds
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
      - Replace=true

---

apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: ray-operator
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/ray-project/kuberay
    targetRevision: v1.5.1  # update this as necessary
    path: helm-chart/kuberay-operator
    helm:
      skipCrds: true   # CRDs are installed in the first Application
  destination:
    server: https://kubernetes.default.svc
    namespace: ray-cluster
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true

---

apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: raycluster
  namespace: argocd
spec:
  project: default
  destination:
    server: https://kubernetes.default.svc
    namespace: ray-cluster
  ignoreDifferences:
    - group: ray.io
      kind: RayCluster
      name: raycluster-kuberay # ensure this is aligned with the release name
      namespace: ray-cluster   # ensure this is aligned with the namespace
      jqPathExpressions:
        - .spec.workerGroupSpecs[].replicas
  source:
    repoURL: https://ray-project.github.io/kuberay-helm/
    chart: ray-cluster
    targetRevision: "1.4.1"
    helm:
      releaseName: raycluster  # this affects the ignoreDifferences field
      valuesObject:
        image:
          repository: docker.io/rayproject/ray
          tag: latest
          pullPolicy: IfNotPresent
        head:
          rayStartParams:
            num-cpus: "0"
          enableInTreeAutoscaling: true
          autoscalerOptions:
            version: v2
            upscalingMode: Default
            idleTimeoutSeconds: 600 # 10 minutes
            env:
            - name: AUTOSCALER_MAX_CONCURRENT_LAUNCHES
              value: "100"
        worker:
          groupName: standard-worker
          replicas: 1
          minReplicas: 1
          maxReplicas: 200
          rayStartParams:
            resources: '"{\"standard-worker\": 1}"'
          resources:
            requests:
              cpu: "1"
              memory: "1G"
        additionalWorkerGroups:
          additional-worker-group1:
            image:
              repository: docker.io/rayproject/ray
              tag: latest
              pullPolicy: IfNotPresent
            disabled: false
            replicas: 1
            minReplicas: 1
            maxReplicas: 30
            rayStartParams:
              resources: '"{\"additional-worker-group1\": 1}"'
            resources:
              requests:
                cpu: "1"
                memory: "1G"
          additional-worker-group2:
            image:
              repository: docker.io/rayproject/ray
              tag: latest
              pullPolicy: IfNotPresent
            disabled: false
            replicas: 1
            minReplicas: 1
            maxReplicas: 200
            rayStartParams:
              resources: '"{\"additional-worker-group2\": 1}"'
            resources:
              requests:
                cpu: "1"
                memory: "1G"
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

Note in this example, the `jqPathExpressions` approach is used.

Apply all three Applications at once:

```sh
kubectl apply -f ray-argocd-all.yaml
```

This single-file approach is convenient for quick deployments, but the step-by-step approach in the earlier sections provides better visibility into the deployment process and makes it easier to troubleshoot issues.
