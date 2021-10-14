#!/bin/bash
set -x
kubectl create namespace basic-test
kubectl apply -f ../../deploy/charts/ray/crds/cluster_crd.yaml
KUBERNETES_OPERATOR_TEST_NAMESPACE=basic-test KUBERNETES_OPERATOR_TEST_IMAGE="$IMAGE" python ../../python/ray/tests/kubernetes_e2e/test_k8s_operator_basic.py
kubectl -n basic-test delete --all rayclusters
kubectl -n basic-test delete deployment ray-operator
kubectl delete namespace basic-test
kubectl delete -f ../../deploy/charts/ray/crds/cluster_crd.yaml
