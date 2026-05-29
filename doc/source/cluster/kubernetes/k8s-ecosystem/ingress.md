(kuberay-ingress)=

# Ingress

The following examples show how to use Ingress or Gateway to access your Ray clusters:

  * [AWS Application Load Balancer (ALB) Ingress support on AWS EKS](kuberay-aws-alb)
  * [GKE Ingress support](kuberay-gke-ingress)
  * [GKE Gateway API support](kuberay-gke-gateway)
  * [Manually setting up NGINX Ingress on Kind](kuberay-nginx)
  * [Azure Application Gateway for Containers Gateway API support on AKS](kuberay-aks-agc)


```{admonition} Warning
:class: warning
**Only expose Ingresses or Gateways to authorized users.** The Ray Dashboard provides read and write access to the Ray Cluster. Anyone with access to this Ingress or Gateway can execute arbitrary code on the Ray Cluster.
```


(kuberay-aws-alb)=
## AWS Application Load Balancer (ALB) Ingress support on AWS EKS

### Prerequisites
* Create an EKS cluster. See [Getting started with Amazon EKS – AWS Management Console and AWS CLI](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-console.html#eks-configure-kubectl).

* Set up the [AWS Load Balancer controller](https://github.com/kubernetes-sigs/aws-load-balancer-controller), see [installation instructions](https://kubernetes-sigs.github.io/aws-load-balancer-controller/latest/deploy/installation/). Note that the repository maintains a webpage for each release. Confirm that you are using the latest installation instructions.

* (Optional) Try the [echo server example](https://github.com/kubernetes-sigs/aws-load-balancer-controller/blob/main/docs/examples/echo_server.md) in the [aws-load-balancer-controller](https://github.com/kubernetes-sigs/aws-load-balancer-controller) repository.

* (Optional) Read [how-it-works.md](https://github.com/kubernetes-sigs/aws-load-balancer-controller/blob/main/docs/how-it-works.md) to understand the [aws-load-balancer-controller](https://github.com/kubernetes-sigs/aws-load-balancer-controller) mechanism.

### Instructions
```sh
# Step 1: Install KubeRay operator and CRD
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm install kuberay-operator kuberay/kuberay-operator --version 1.6.0

# Step 2: Install a RayCluster
helm install raycluster kuberay/ray-cluster --version 1.6.0

# Step 3: Edit the `ray-operator/config/samples/ray-cluster-alb-ingress.yaml`
#
# (1) Annotation `alb.ingress.kubernetes.io/subnets`
#   1. Please include at least two subnets.
#   2. One Availability Zone (ex: us-west-2a) can only have at most 1 subnet.
#   3. In this example, you need to select public subnets (subnets that "Auto-assign public IPv4 address" is Yes on AWS dashboard)
#
# (2) Set the name of head pod service to `spec...backend.service.name`
eksctl get cluster ${YOUR_EKS_CLUSTER} # Check subnets on the EKS cluster

# Step 4: Check ingress created by Step 4.
kubectl describe ingress ray-cluster-ingress

# [Example]
# Name:             ray-cluster-ingress
# Labels:           <none>
# Namespace:        default
# Address:          k8s-default-rayclust-....${REGION_CODE}.elb.amazonaws.com
# Default backend:  default-http-backend:80 (<error: endpoints "default-http-backend" not found>)
# Rules:
#  Host        Path  Backends
#  ----        ----  --------
#  *
#              /   ray-cluster-kuberay-head-svc:8265 (192.168.185.157:8265)
# Annotations: alb.ingress.kubernetes.io/scheme: internal
#              alb.ingress.kubernetes.io/subnets: ${SUBNET_1},${SUBNET_2}
#              alb.ingress.kubernetes.io/tags: Environment=dev,Team=test
#              alb.ingress.kubernetes.io/target-type: ip
# Events:
#   Type    Reason                  Age   From     Message
#   ----    ------                  ----  ----     -------
#   Normal  SuccessfullyReconciled  39m   ingress  Successfully reconciled

# Step 6: Check ALB on AWS (EC2 -> Load Balancing -> Load Balancers)
#        The name of the ALB should be like "k8s-default-rayclust-......".

# Step 7: Check Ray Dashboard by ALB DNS Name. The name of the DNS Name should be like
#        "k8s-default-rayclust-.....us-west-2.elb.amazonaws.com"

# Step 8: Delete the ingress, and AWS Load Balancer controller will remove ALB.
#        Check ALB on AWS to make sure it is removed.
kubectl delete ingress ray-cluster-ingress
```

(kuberay-gke-ingress)=

## GKE Ingress support

### Prerequisites

* Create a GKE cluster and ensure that you have the kubectl tool installed and authenticated to communicate with your GKE cluster.  See [this tutorial](kuberay-gke-gpu-cluster-setup) for an example of how to create a GKE cluster with GPUs.  (GPUs are not necessary for this section.)

* If you are using a `gce-internal` ingress, create a [Proxy-Only subnet](https://docs.cloud.google.com/load-balancing/docs/proxy-only-subnets#proxy_only_subnet_create) in the same region as your GKE cluster.

* It may be helpful to understand the concepts at <https://docs.cloud.google.com/kubernetes-engine/docs/concepts/ingress>.

### Instructions
Save the following file as `ray-cluster-gclb-ingress.yaml`:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: ray-cluster-ingress
  annotations:
    kubernetes.io/ingress.class: "gce-internal"
spec:
  rules:
    - http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: raycluster-kuberay-head-svc # Update this line with your head service in Step 3 below.
                port:
                  number: 8265
```

Now run the following commands:

```bash
# Step 1: Install KubeRay operator and CRD
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm install kuberay-operator kuberay/kuberay-operator --version 1.6.0

# Step 2: Install a RayCluster. GKE Ingress requires the backend service to be of type NodePort.
helm install raycluster kuberay/ray-cluster --version 1.6.0 --set service.type=NodePort

# Step 3: Edit ray-cluster-gclb-ingress.yaml to replace the service name with the name of the head service from the RayCluster. (Output of `kubectl get svc`)

# Step 4: Apply the Ingress configuration
kubectl apply -f ray-cluster-gclb-ingress.yaml

# Step 5: Check ingress created by Step 4.
kubectl describe ingress ray-cluster-ingress

# Step 6: After a few minutes, GKE allocates an internal IP for the ingress. Check it using:
kubectl get ingress ray-cluster-ingress

# Example output:
# NAME                  CLASS          HOSTS   ADDRESS     PORTS   AGE
# ray-cluster-ingress   gce-internal   *       10.0.1.15   80      54m

# Step 7: Check Ray Dashboard. Since this is an internal Ingress, the IP is only accessible from within the VPC. To access the Ingress from your local machine, you must use a VPN, a proxy, or a VM inside the same VPC network.

# Step 8: Delete the ingress.
kubectl delete ingress ray-cluster-ingress
```

(kuberay-gke-gateway)=
## GKE Gateway API support

### Prerequisites

* Create a [GKE cluster with Gateway API enabled](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/deploying-gateways#enable-gateway). Ensure that you have the `kubectl` tool installed and authenticated to communicate with your GKE cluster.
  * Gateway API is enabled by default for GKE Autopilot. For GKE Standard, you may need to enable it. See [Enabling Gateway API](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/deploying-gateways#enable-gateway) for instructions.

* If you are using the `gke-l7-rilb` Gateway Class for a private internal-only Gateway, create a [Proxy-Only subnet](https://cloud.google.com/load-balancing/docs/proxy-only-subnets#proxy_only_subnet_create) in the same region as your GKE cluster in the VPC network.

* It may be helpful to understand the concepts at <https://cloud.google.com/kubernetes-engine/docs/concepts/gateway-api>.

### Instructions
Save the following file as `ray-cluster-gke-gateway.yaml`:

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: ray-cluster-gateway
spec:
  gatewayClassName: gke-l7-rilb # Use "gke-l7-global-external-managed" instead if you want to create a public, external Gateway.
  listeners:
  - name: http
    protocol: HTTP
    port: 80
    allowedRoutes:
      namespaces:
        from: Same
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: ray-cluster-http-route
spec:
  parentRefs:
  - name: ray-cluster-gateway
  rules:
  - backendRefs:
    - name: raycluster-kuberay-head-svc # Update this line with your head service in Step 3 below.
      port: 8265
```

```{admonition} Warning
:class: warning
Exposing the Ray Dashboard provides cluster access, which allows executing arbitrary code. If you configure a public, external Gateway (using `gke-l7-global-external-managed`), ensure that you configure proper authentication and authorization. For details on setting up SSL/TLS, Google Cloud Armor, and Identity-Aware Proxy (IAP), see the Google Cloud guide on [Securing a Gateway](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/secure-gateway).
```

Now run the following commands:

```bash
# Step 1: Install KubeRay operator and CRD
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm install kuberay-operator kuberay/kuberay-operator --version 1.6.0

# Step 2: Install a RayCluster
helm install raycluster kuberay/ray-cluster --version 1.6.0

# Step 3: Edit ray-cluster-gke-gateway.yaml to replace the service name with the name of the head service from the RayCluster. (Output of `kubectl get svc`)

# Step 4: Apply the Gateway and HTTPRoute configuration
kubectl apply -f ray-cluster-gke-gateway.yaml

# Step 5: Check Gateway created by Step 4.
kubectl describe gateway ray-cluster-gateway

# Step 6: Wait for GKE to allocate an internal IP and program the Gateway.
kubectl wait --for=condition=Programmed gateway/ray-cluster-gateway --timeout=5m
kubectl get gateway ray-cluster-gateway

# Example output:
# NAME                  CLASS         ADDRESS      PROGRAMMED   AGE
# ray-cluster-gateway   gke-l7-rilb   10.0.1.15    True         54m

# Step 7: Check Ray Dashboard. Since this is an internal Gateway, the IP is only accessible from within the VPC. To access the Gateway from your local machine, you must use a VPN, a proxy, or a VM inside the same VPC network.

# Step 8: Delete the gateway and HTTPRoute.
kubectl delete -f ray-cluster-gke-gateway.yaml
```

```{note}
This guide focuses on exposing the Ray Dashboard and API on a single GKE cluster. For deploying multi-cluster Ray serving architectures, see the Google Cloud guide on [serving multi-cluster Ray inference using a Gateway](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/serve-multi-cluster-ray-inference-gateway).
```

(kuberay-nginx)=
## Manually setting up NGINX Ingress on Kind

```sh
# Step 1: Create a Kind cluster with `extraPortMappings` and `node-labels`
# Reference for the setting up of Kind cluster: https://kind.sigs.k8s.io/docs/user/ingress/
cat <<EOF | kind create cluster --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  kubeadmConfigPatches:
  - |
    kind: InitConfiguration
    nodeRegistration:
      kubeletExtraArgs:
        node-labels: "ingress-ready=true"
  extraPortMappings:
  - containerPort: 80
    hostPort: 80
    protocol: TCP
  - containerPort: 443
    hostPort: 443
    protocol: TCP
EOF

# Step 2: Install NGINX ingress controller
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml
sleep 10 # Wait for the Kubernetes API Server to create the related resources
kubectl wait --namespace ingress-nginx \
  --for=condition=ready pod \
  --selector=app.kubernetes.io/component=controller \
  --timeout=90s

# Step 3: Install KubeRay operator and CRD
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm install kuberay-operator kuberay/kuberay-operator --version 1.6.0

# Step 4: Install RayCluster and create an ingress separately.
# More information about change of setting was documented in https://github.com/ray-project/kuberay/pull/699
# and `ray-operator/config/samples/ray-cluster.separate-ingress.yaml`
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/v1.6.0/ray-operator/config/samples/ray-cluster.separate-ingress.yaml
kubectl apply -f ray-cluster.separate-ingress.yaml

# Step 5: Check the ingress created in Step 4.
kubectl describe ingress raycluster-ingress-head-ingress

# [Example]
# ...
# Rules:
# Host        Path  Backends
# ----        ----  --------
# *
#             /raycluster-ingress/(.*)   raycluster-ingress-head-svc:8265 (10.244.0.11:8265)
# Annotations:  nginx.ingress.kubernetes.io/rewrite-target: /$1

# Step 6: Check `<ip>/raycluster-ingress/` on your browser. You will see the Ray Dashboard.
#        [Note] The forward slash at the end of the address is necessary. `<ip>/raycluster-ingress`
#               will report "404 Not Found".
```

(kuberay-aks-agc)=
## Azure Application Gateway for Containers Gateway API support on AKS

### Prerequisites
* Create an AKS cluster. See [Quickstart: Deploy an Azure Kubernetes Service (AKS) cluster using Azure CLI](https://learn.microsoft.com/azure/aks/learn/quick-kubernetes-deploy-cli).

* Deploy Application Gateway for Containers ALB Controller [Quickstart: Deploy Application Gateway for Containers ALB Controller](https://learn.microsoft.com/azure/application-gateway/for-containers/quickstart-deploy-application-gateway-for-containers-alb-controller?tabs=install-helm-windows).

* Deploy Application Gateway for Containers [Quickstart: Create Application Gateway for Containers managed by ALB Controller](https://learn.microsoft.com/azure/application-gateway/for-containers/quickstart-create-application-gateway-for-containers-managed-by-alb-controller?tabs=new-subnet-aks-vnet) 

* (Optional) Read [What is Application Gateway for Containers](https://learn.microsoft.com/azure/application-gateway/for-containers/overview).

* (Optional) Read [Secure your web applications with Azure Web Application Firewall on Application Gateway for Containers](https://learn.microsoft.com/azure/application-gateway/for-containers/web-application-firewall)

### Instructions
```sh
# Step 1: Install KubeRay operator and CRD
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm install kuberay-operator kuberay/kuberay-operator --version 1.6.0

# Step 2: Install a RayCluster
helm install raycluster kuberay/ray-cluster --version 1.6.0

# Step 3: Edit the `ray-operator/config/samples/ray-cluster-agc-gatewayapi.yaml`
#
# (1) Annotation `alb.networking.azure.io/alb-namespace`
#   1. Please update this to the namespace of your alb custom resource.
#
# (2) Annotation `alb.networking.azure.io/alb-name`
#   1. Please update this to the name of your alb custom resource.

# Step 4: Check gateway and http route created by Step 3.
kubectl describe gateway ray-cluster-gateway

# [Example]
# Name:         ray-cluster-gateway
# Namespace:    default
# Labels:       <none>
# Annotations:  
#   alb.networking.azure.io/alb-namespace: alb-test-infra
#   alb.networking.azure.io/alb-name: alb-test
# API Version:  gateway.networking.k8s.io/v1
# Kind:         Gateway
# Metadata:
#   Creation Timestamp:  2025-09-12T04:44:18Z
#   Generation:          1
#   Resource Version:    247986
#   UID:                 88c40c06-83fe-4ef3-84e1-7bc36c9b5b43
# Spec:
#   Gateway Class Name:  azure-alb-external
#   Listeners:
#     Allowed Routes:
#       Namespaces:
#         From:  Same
#     Name:      http
#     Port:      80
#     Protocol:  HTTP
# Status:
#   Addresses:
#     Type:   Hostname
#     Value:  xxxx.yyyy.alb.azure.com
#   Conditions:
#     Last Transition Time:  2025-09-12T04:49:30Z
#     Message:               Valid Gateway
#     Observed Generation:   1
#     Reason:                Accepted
#     Status:                True
#     Type:                  Accepted
#     Last Transition Time:  2025-09-12T04:49:30Z
#     Message:               Application Gateway for Containers resource has been successfully updated.
#     Observed Generation:   1
#     Reason:                Programmed
#     Status:                True
#     Type:                  Programmed
#   Listeners:
#     Attached Routes:  1
#     Conditions:
#       Last Transition Time:  2025-09-12T04:49:30Z
#       Message:
#       Observed Generation:   1
#       Reason:                ResolvedRefs
#       Status:                True
#       Type:                  ResolvedRefs
#       Last Transition Time:  2025-09-12T04:49:30Z
#       Message:               Listener is Accepted
#       Observed Generation:   1
#       Reason:                Accepted
#       Status:                True
#       Type:                  Accepted
#       Last Transition Time:  2025-09-12T04:49:30Z
#       Message:               Application Gateway for Containers resource has been successfully updated.
#       Observed Generation:   1
#       Reason:                Programmed
#       Status:                True
#       Type:                  Programmed
#     Name:                    http
#     Supported Kinds:
#       Group:  gateway.networking.k8s.io
#       Kind:   HTTPRoute
#       Group:  gateway.networking.k8s.io
#       Kind:   GRPCRoute
# Events:       <none>

kubectl describe httproutes ray-cluster-http-route

# [Example]
# Name:         ray-cluster-http-route
# Namespace:    default
# Labels:       <none>
# Annotations:  <none>
# API Version:  gateway.networking.k8s.io/v1
# Kind:         HTTPRoute
# Metadata:
#   Creation Timestamp:  2025-09-12T04:44:43Z
#   Generation:          2
#   Resource Version:    247982
#   UID:                 54bbd1e6-bd28-4cae-a469-e15105f077b8
# Spec:
#   Parent Refs:
#     Group:  gateway.networking.k8s.io
#     Kind:   Gateway
#     Name:   ray-cluster-gateway
#   Rules:
#     Backend Refs:
#       Group:
#       Kind:    Service
#       Name:    raycluster-kuberay-head-svc
#       Port:    8265
#       Weight:  1
#     Matches:
#       Path:
#         Type:   PathPrefix
#         Value:  /
# Status:
#   Parents:
#     Conditions:
#       Last Transition Time:  2025-09-12T04:49:30Z
#       Message:
#       Observed Generation:   2
#       Reason:                ResolvedRefs
#       Status:                True
#       Type:                  ResolvedRefs
#       Last Transition Time:  2025-09-12T04:49:30Z
#       Message:               Route is Accepted
#       Observed Generation:   2
#       Reason:                Accepted
#       Status:                True
#       Type:                  Accepted
#       Last Transition Time:  2025-09-12T04:49:30Z
#       Message:               Application Gateway for Containers resource has been successfully updated.
#       Observed Generation:   2
#       Reason:                Programmed
#       Status:                True
#       Type:                  Programmed
#     Controller Name:         alb.networking.azure.io/alb-controller
#     Parent Ref:
#       Group:  gateway.networking.k8s.io
#       Kind:   Gateway
#       Name:   ray-cluster-gateway
# Events:       <none>

# Step 5: Check Ray Dashboard by visiting the FQDN assigned to your gateway object in your browser
#        FQDN can be obtained by the command:
#        kubectl get gateway ray-cluster-gateway -o jsonpath='{.status.addresses[0].value}'       

# Step 6: Delete the gateway and http route
kubectl delete gateway ray-cluster-gateway
kubectl delete httproutes ray-cluster-http-route

# Step 7: Delete Application Gateway for containers
kubectl delete applicationloadbalancer alb-test -n alb-test-infra
kubectl delete ns alb-test-infra
```
