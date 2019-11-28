# Ray-Operator Documentation

Kubernetes is an open-source system for automating deployment, scaling, and management of containerized applications, It is playing an important role in the next generation cloud-infra. 

So running Ray on Kubernetes will make Ray more acceptable to the industry.  

The CR(Custom Resource defined in Kubernetes) and the controller we propose allow users to customize almost all the Kubernetes and Ray configurations. 

The solution provides the following features:
- A CRD of RayCluster used for the desired state.
- Support Ray cluster with heterogeneous Pods, CPU/MEM and other attributes.
- Update the desired state following the Declarative API design concept.
- HA for Ray Operator.

## File structure:
> ```
> ray/deploy/ray-operator
> ├── api/v1  // Package v1 contains API Schema definitions for the ray v1 API group
> │   ├── groupversion_info.go 
> │   ├── raycluster_types.go  // RayCluster field definitions
> │   └── zz_generated.deepcopy.go // RayCluster field built-in function
> │   
> └── config  // Kubernetes require Config 
>    ├── certmanager  // self-signed issuer CR and a certificate CR.
>    ├── crd          // crd and related config
>    ├── default
>    ├── manager      // manager config in Kubernetes
>    ├── prometheus         
>    ├── rbac
>    ├── samples      // sample RayCluster yaml
>    └── webhook
> ```

## RayCluster sample CR

To introduce the Ray-Operator, give an explanation of RayCluster CR firstly.  

[RayCluster.mini.yaml](config/samples/ray_v1_raycluster.mini.yaml)         - 4 pods in this sample, 1 for head and 3 for workers but with different specifications.

[RayCluster.complete.yaml](config/samples/ray_v1_raycluster.complete.yaml) - a complete version CR for Customized requirement.

## RayCluster CRD

Refers to file [raycluster_types.go](api/v1/raycluster_types.go) for code details.

If interested in CRD, refer to file [CRD](config/crd/bases/ray.io_rayclusters.yaml) for more details. 



## Software requirement
Take care some software have dependency.  

software  | version | memo
:-------------  | :---------------:| -------------:
kustomize |  v3.1.0+ | [download](https://github.com/kubernetes-sigs/kustomize)
kubectl |  v1.11.3+    | [download](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
Kubernetes Cluster | Access to a Kubernetes v1.11.3+ cluster| [Minikube](https://github.com/kubernetes/minikube)  for local test
go  | v1.13+|[download](https://golang.org/dl/)
docker   | 17.03+|[download](https://docs.docker.com/install/)

Also you will need kubeconfig in ~/.kube/config, so you can access to Kubernetes Cluster.  

## Get started
Below gives a guide for user to submit RayCluster step by step:

### Install CRDs into a cluster

```shell script
kustomize build config/crd | kubectl apply -f -
```

### Deploy controller in the configured Kubernetes cluster in ~/.kube/config
```shell script
cd config/manager 
kustomize build config/default | kubectl apply -f -
```

### Submit RayCluster to Kubernetes
```shell script
kubectl create -f config/samples/ray_v1_raycluster.mini.yaml
```

### Apply RayCluster to Kubernetes
```shell script
kubectl apply -f config/samples/ray_v1_raycluster.mini.yaml
```

### Delete RayCluster to Kubernetes
```shell script
kubectl delete -f config/samples/ray_v1_raycluster.mini.yaml
```