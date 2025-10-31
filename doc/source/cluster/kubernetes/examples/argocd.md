(deploying-on-argocd-example)=

# Deploying via ArgoCD

Below is an example template on how to deploy using ArgoCD with
3 different worker groups:

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
    targetRevision: v1.4.0  # update this as necessary
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
    targetRevision: v1.4.0  # update this as necessary
    path: helm-chart/kuberay-operator
    helm:
      skipCrds: true   # note this step is required
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
      name: raycluster-kuberay
      namespace: ray-cluster
      jsonPointers:
        - /spec/workerGroupSpecs/0/replicas
        - /spec/workerGroupSpecs/1/replicas
        - /spec/workerGroupSpecs/2/replicas
  source:
    repoURL: https://ray-project.github.io/kuberay-helm/
    chart: ray-cluster
    targetRevision: "1.4.1"
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
        additionalWorkerGroups:
          additional-worker-group1:
            image:
              repository: docker.io/rayproject/ray
              tag: latest
              pullPolicy: IfNotPresent
            disabled: false
            replicas: 0
            minReplicas: 0
            maxReplicas: 30
            rayStartParams:
              resources: '"{\"additional-worker-group1\": 1}"'
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
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true

```

## Auto Scaling

With regard to the Ray autoscaler, note this section in the ArgoCD application:

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

It has been observed that without this `ignoreDifferences` section, ArgoCD
and the Ray Autoscaler may conflict, resulting in unexpected behaviour when
it comes to requesting workers dynamically (e.g. `ray.autoscaler.sdk.request_resources`).
More specifically, when requesting N workers, the Autoscaler would not spin up N workers.