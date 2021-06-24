set -x
kubectl create namespace helm-test
kubectl create namespace helm-test2
#KUBERNETES_OPERATOR_PULL_POLICY=IfNotPresent
KUBERNETES_OPERATOR_TEST_NAMESPACE=helm-test KUBERNETES_OPERATOR_TEST_IMAGE="$IMAGE" python ../ray/python/ray/tests/kubernetes_e2e/test_helm.py
kubectl delete namespace helm-test
kubectl delete namespace helm-test2
kubectl delete -f ../ray/deploy/charts/ray/crds/cluster_crd.yaml
