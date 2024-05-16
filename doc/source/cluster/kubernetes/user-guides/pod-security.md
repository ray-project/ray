(kuberay-pod-security)=

# Pod Security

Kubernetes defines three different Pod Security Standards, including `privileged`, `baseline`, and `restricted`, to broadly
cover the security spectrum. The `privileged` standard allows users to do known privilege escalations, and thus it is not 
safe enough for security-critical applications.

This document describes how to configure RayCluster YAML file to apply `restricted` Pod security standard. The following 
references can help you understand this document better:

* [Kubernetes - Pod Security Standards](https://kubernetes.io/docs/concepts/security/pod-security-standards/#restricted)
* [Kubernetes - Pod Security Admission](https://kubernetes.io/docs/concepts/security/pod-security-admission/)
* [Kubernetes - Auditing](https://kubernetes.io/docs/tasks/debug/debug-cluster/audit/)
* [Kind - Auditing](https://kind.sigs.k8s.io/docs/user/auditing/)

# Preparation

Please clone the [KubeRay repository](https://github.com/ray-project/kuberay) and checkout the `master` branch.
This tutorial requires several files in the repository.

# Step 1: Create a Kind cluster

```bash
# Path: kuberay/
kind create cluster --config ray-operator/config/security/kind-config.yaml --image=kindest/node:v1.26.0
```

The `kind-config.yaml` enables audit logging with the audit policy defined in `audit-policy.yaml`. The `audit-policy.yaml`
defines an auditing policy to listen to the Pod events in the namespace `pod-security`. With this policy, we can check
whether our Pods violate the policies in `restricted` standard or not.

The feature [Pod Security Admission](https://kubernetes.io/docs/concepts/security/pod-security-admission/) is firstly 
introduced in Kubernetes v1.22 (alpha) and becomes stable in Kubernetes v1.25. In addition, KubeRay currently supports 
Kubernetes from v1.19 to v1.24. (At the time of writing, we have not tested KubeRay with Kubernetes v1.25). Hence, I use **Kubernetes v1.24** in this step.

# Step 2: Check the audit logs

```bash
docker exec kind-control-plane cat /var/log/kubernetes/kube-apiserver-audit.log
```
The log should be empty because the namespace `pod-security` does not exist.

# Step 3: Create the `pod-security` namespace

```bash
kubectl create ns pod-security
kubectl label --overwrite ns pod-security \
  pod-security.kubernetes.io/warn=restricted \
  pod-security.kubernetes.io/warn-version=latest \
  pod-security.kubernetes.io/audit=restricted \
  pod-security.kubernetes.io/audit-version=latest \
  pod-security.kubernetes.io/enforce=restricted \
  pod-security.kubernetes.io/enforce-version=latest
```

With the `pod-security.kubernetes.io` labels, the built-in Kubernetes Pod security admission controller will apply the 
`restricted` Pod security standard to all Pods in the namespace `pod-security`. The label
`pod-security.kubernetes.io/enforce=restricted` means that the Pod will be rejected if it violate the policies defined in 
`restricted` security standard. See [Pod Security Admission](https://kubernetes.io/docs/concepts/security/pod-security-admission/) for more details about the labels.

# Step 4: Install the KubeRay operator

```bash
# Update the field securityContext in helm-chart/kuberay-operator/values.yaml
securityContext:
  allowPrivilegeEscalation: false
  capabilities:
    drop: ["ALL"]
  runAsNonRoot: true
  seccompProfile:
    type: RuntimeDefault

# Path: kuberay/helm-chart/kuberay-operator
helm install -n pod-security kuberay-operator .
```

# Step 5: Create a RayCluster (Choose either Step 5.1 or Step 5.2)

* If you choose Step 5.1, no Pod will be created in the namespace `pod-security`.
* If you choose Step 5.2, Pods can be created successfully.

## Step 5.1: Create a RayCluster without proper `securityContext` configurations

```bash
# Path: kuberay/ray-operator/config/samples
kubectl apply -n pod-security -f ray-cluster.complete.yaml

# Wait 20 seconds and check audit logs for the error messages.
docker exec kind-control-plane cat /var/log/kubernetes/kube-apiserver-audit.log

# Example error messagess
# "pods \"raycluster-complete-head-fkbf5\" is forbidden: violates PodSecurity \"restricted:latest\": allowPrivilegeEscalation != false (container \"ray-head\" must set securityContext.allowPrivilegeEscalation=false) ...

kubectl get pod -n pod-security
# NAME                               READY   STATUS    RESTARTS   AGE
# kuberay-operator-8b6d55dbb-t8msf   1/1     Running   0          62s

# Clean up the RayCluster
kubectl delete rayclusters.ray.io -n pod-security raycluster-complete
# raycluster.ray.io "raycluster-complete" deleted
```

No Pod will be created in the namespace `pod-security`, and check audit logs for error messages.

## Step 5.2: Create a RayCluster with proper `securityContext` configurations

```bash
# Path: kuberay/ray-operator/config/security
kubectl apply -n pod-security -f ray-cluster.pod-security.yaml

# Wait for the RayCluster convergence and check audit logs for the messages.
docker exec kind-control-plane cat /var/log/kubernetes/kube-apiserver-audit.log

# Forward the dashboard port
kubectl port-forward svc/raycluster-pod-security-head-svc -n pod-security 8265:8265

# Log in to the head Pod
kubectl exec -it -n pod-security ${YOUR_HEAD_POD} -- bash

# (Head Pod) Run a sample job in the Pod
python3 samples/xgboost_example.py

# Check the job status in the dashboard on your browser.
# http://127.0.0.1:8265/#/job => The job status should be "SUCCEEDED".

# (Head Pod) Make sure Python dependencies can be installed under `restricted` security standard 
pip3 install jsonpatch
echo $? # Check the exit code of `pip3 install jsonpatch`. It should be 0.

# Clean up the RayCluster
kubectl delete -n pod-security -f ray-cluster.pod-security.yaml
# raycluster.ray.io "raycluster-pod-security" deleted
# configmap "xgboost-example" deleted
```

One head Pod and one worker Pod will be created as specified in `ray-cluster.pod-security.yaml`.
First, we log in to the head Pod, run a XGBoost example script, and check the job
status in the dashboard. Next, we use `pip` to install a Python dependency (i.e. `jsonpatch`), and the exit code of the `pip` command should be 0.
