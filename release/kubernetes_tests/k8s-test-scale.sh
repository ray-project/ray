#!/bin/bash
set -x
kubectl create namespace scale-test
kubectl create namespace scale-test2
KUBERNETES_OPERATOR_TEST_NAMESPACE=scale-test KUBERNETES_OPERATOR_TEST_IMAGE="$IMAGE" python ../../python/ray/tests/kubernetes_e2e/test_k8s_operator_scaling.py
kubectl -n scale-test delete --all rayclusters
kubectl -n scale-test2 delete --all rayclusters
kubectl delete -f ../../deploy/components/operator_cluster_scoped.yaml
kubectl delete namespace scale-test
kubectl delete namespace scale-test2
kubectl delete -f ../../deploy/charts/ray/crds/cluster_crd.yaml
