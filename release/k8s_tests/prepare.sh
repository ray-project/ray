#!/usr/bin/env bash

set -ex

aws secretsmanager get-secret-value \
    --secret-id oss-nightly-ci-gke-cluster-service-account \
    --region us-west-2 |
    jq -r .SecretString |
    jq -r .json_text \
        >gke_service_account.json

gcloud auth activate-service-account \
    anyscale-ci@oss-nightly-test.iam.gserviceaccount.com \
    --key-file=gke_service_account.json

USE_GKE_GCLOUD_AUTH_PLUGIN=True \
    gcloud container clusters get-credentials \
    serve-ha-nightly \
    --zone us-central1-c \
    --project oss-nightly-test

kubectl get nodes
