set -x
kubectl create namespace basic-test
kubectl apply -f ../ray/deploy/charts/ray/crds/cluster_crd.yaml
#KUBERNETES_OPERATOR_PULL_POLICY=IfNotPresent
KUBERNETES_OPERATOR_TEST_NAMESPACE=basic-test KUBERNETES_OPERATOR_TEST_IMAGE="$IMAGE" python ../ray/python/ray/tests/kubernetes_e2e/test_k8s_operator_basic.py
kubectl -n basic-test delete --all rayclusters
kubectl -n basic-test delete deployment ray-operator
kubectl delete namespace basic-test
kubectl delete -f ../ray/deploy/charts/ray/crds/cluster_crd.yaml
