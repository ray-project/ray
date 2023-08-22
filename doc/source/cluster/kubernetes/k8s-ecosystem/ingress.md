(kuberay-ingress)=

# Ingress

Here we provide some examples to show how to use ingress to access your Ray cluster.

  * [Example: AWS Application Load Balancer (ALB) Ingress support on AWS EKS](#example-aws-application-load-balancer-alb-ingress-support-on-aws-eks)
  * [Example: Manually setting up NGINX Ingress on KinD](#example-manually-setting-up-nginx-ingress-on-kind)

## Example: AWS Application Load Balancer (ALB) Ingress support on AWS EKS

### Prerequisite
* Follow the document [Getting started with Amazon EKS – AWS Management Console and AWS CLI](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-console.html#eks-configure-kubectl) to create an EKS cluster.

* Follow the [installation instructions](https://kubernetes-sigs.github.io/aws-load-balancer-controller/latest/deploy/installation/) to set up the [AWS Load Balancer controller](https://github.com/kubernetes-sigs/aws-load-balancer-controller). Note that the repository maintains a webpage for each release. Please make sure you use the latest installation instructions.

* (Optional) Try [echo server example](https://github.com/kubernetes-sigs/aws-load-balancer-controller/blob/main/docs/examples/echo_server.md) in the [aws-load-balancer-controller](https://github.com/kubernetes-sigs/aws-load-balancer-controller) repository.

* (Optional) Read [how-it-works.md](https://github.com/kubernetes-sigs/aws-load-balancer-controller/blob/main/docs/how-it-works.md) to understand the mechanism of [aws-load-balancer-controller](https://github.com/kubernetes-sigs/aws-load-balancer-controller).

### Instructions
```sh
# Step 1: Install KubeRay operator and CRD
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm install kuberay-operator kuberay/kuberay-operator --version 0.6.0

# Step 2: Install a RayCluster
helm install raycluster kuberay/ray-cluster --version 0.6.0

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
# Annotations: alb.ingress.kubernetes.io/scheme: internet-facing
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

## Example: Manually setting up NGINX Ingress on KinD

```sh
# Step 1: Create a KinD cluster with `extraPortMappings` and `node-labels`
# Reference for the setting up of kind cluster: https://kind.sigs.k8s.io/docs/user/ingress/
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
helm install kuberay-operator kuberay/kuberay-operator --version 0.6.0

# Step 4: Install RayCluster and create an ingress separately.
# More information about change of setting was documented in https://github.com/ray-project/kuberay/pull/699 
# and `ray-operator/config/samples/ray-cluster.separate-ingress.yaml`
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-cluster.separate-ingress.yaml
kubectl apply -f ray-operator/config/samples/ray-cluster.separate-ingress.yaml

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
