#!/bin/bash
set -x
kubectl create namespace helm-test
kubectl create namespace helm-test2
KUBERNETES_OPERATOR_TEST_NAMESPACE=helm-test KUBERNETES_OPERATOR_TEST_IMAGE="$IMAGE" python ../../python/ray/tests/kubernetes_e2e/test_helm.py
kubectl delete namespace helm-test
kubectl delete namespace helm-test2
kubectl delete -f ../../deploy/charts/ray/crds/cluster_crd.yaml
