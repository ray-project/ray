(deploy-a-static-ray-cluster-without-kuberay)=

# (Advanced) Deploying a static Ray cluster without KubeRay

This deployment method for Ray no longer requires the use of CustomResourceDefinitions (CRDs).
In contrast, the CRDs is a prerequisite to use KubeRay. One of its key components, the Kuberay operator,
manages the Ray cluster resources by watching for Kubernetes events (create/delete/update).
Although the KubeRay operator can function within a single namespace, the use of CRDs has a cluster-wide scope.
If the necessary Kubernetes admin permissions are not available for deploying KubeRay, a static Ray cluster
can still be deployed to Kubernetes. However, it should be noted that this deployment method lacks the built-in
autoscaling feature that KubeRay provides.
