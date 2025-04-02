#!/bin/bash

set -euo pipefail

source anyscale/ci/setup-env.sh

if [[ "${RAY_RELEASE_BUILD:-}" != "true" ]]; then
    # Cleanup temp dir.
    echo "Not a release build, skipping upload the site package."
    exit
fi

DEPLOY_ENVIRONMENT="${1:-development}"

PY_VERSION_CODES=(py39 py310 py311 py312)
WHEEL_PY_VERSION_CODES=(cp39-cp39 cp310-cp310 cp311-cp311 cp312-cp312)

TMP="$(mktemp -d)"

HOSTTYPE="x86_64"

if [[ "${DEPLOY_ENVIRONMENT}" == "staging" ]]; then
    readonly ORG_DATA_BUCKET=anyscale-staging-organization-data-us-west-2
    readonly DEPLOY_ROLE="arn:aws:iam::623395924981:role/buildkite-deploy-to-staging"
elif [[ "${DEPLOY_ENVIRONMENT}" == "predeploy" ]]; then
    readonly ORG_DATA_BUCKET=anyscale-predeploy-organization-data-us-west-2
    readonly DEPLOY_ROLE="arn:aws:iam::521861002309:role/buildkite-deploy-to-predeploy"
elif [[ "${DEPLOY_ENVIRONMENT}" == "production" ]]; then
    readonly ORG_DATA_BUCKET=anyscale-production-organization-data-us-west-2
    readonly DEPLOY_ROLE="arn:aws:iam::525325868955:role/buildkite-deploy-to-production"
elif [[ "${DEPLOY_ENVIRONMENT}" == "development" ]]; then
    readonly ORG_DATA_BUCKET=anyscale-dev-organization-data-us-west-2
    readonly DEPLOY_ROLE="arn:aws:iam::830883877497:role/buildkite-deploy-to-premerge"
else
    echo "Unknown deploy environment: ${DEPLOY_ENVIRONMENT}" >/dev/stderr
    exit 1
fi

echo "--- Upload to org data for ${DEPLOY_ENVIRONMENT}"

for PY_VERSION_CODE in "${PY_VERSION_CODES[@]}"; do
    # All tar.gz's are the same, we only need to upload one of them.
    # so just upload the basic ray:cpu one.
    aws s3 cp "${S3_TEMP}/ray-opt/${PY_VERSION_CODE}/ray-${PY_VERSION_CODE}-cpu.tar.gz" \
        "${TMP}/ray-${PY_VERSION_CODE}-cpu.tar.gz"
    gunzip -k "${TMP}/ray-${PY_VERSION_CODE}-cpu.tar.gz"

    aws s3 cp "${S3_TEMP}/ray-opt/${PY_VERSION_CODE}/ray-${PY_VERSION_CODE}-cpu-min.tar.gz" \
        "${TMP}/ray-${PY_VERSION_CODE}-cpu-min.tar.gz"
    gunzip -k "${TMP}/ray-${PY_VERSION_CODE}-cpu-min.tar.gz"
done

for WHEEL_PY_VERSION_CODE in "${WHEEL_PY_VERSION_CODES[@]}"; do
    WHEEL_FILE="ray-${RAY_VERSION}-${WHEEL_PY_VERSION_CODE}-manylinux2014_${HOSTTYPE}.whl"
    aws s3 cp "${S3_TEMP}/${WHEEL_FILE}" "${TMP}/${WHEEL_FILE}"
done

if [[ "${BUILDKITE:-}" == "true" ]]; then
    eval "$(aws sts assume-role \
        --role-arn "${DEPLOY_ROLE}" \
        --role-session-name "runtime-sitepkg-${RAYCI_BUILD_ID}-${DEPLOY_ENVIRONMENT}" \
        --duration-seconds 900 | python anyscale/ci/assume_role_envs.py)"
fi

RAY_COMMIT="$(git rev-parse HEAD)"

S3_PATH_PREFIX="common/ray-opt/${RAY_VERSION}/${RAY_COMMIT}"

for PY_VERSION_CODE in "${PY_VERSION_CODES[@]}"; do
    # Must keep this consistent with the image.
    ANYSCALE_PRESTART_DATA_PATH_TARGZ="${S3_PATH_PREFIX}/ray-opt-${PY_VERSION_CODE}.tar.gz"
    ANYSCALE_PRESTART_DATA_PATH_TAR="${S3_PATH_PREFIX}/ray-opt-${PY_VERSION_CODE}.tar"

    aws s3 cp "${TMP}/ray-${PY_VERSION_CODE}-cpu.tar.gz" \
        "s3://${ORG_DATA_BUCKET}/${ANYSCALE_PRESTART_DATA_PATH_TARGZ}"
    aws s3 cp "${TMP}/ray-${PY_VERSION_CODE}-cpu.tar" \
        "s3://${ORG_DATA_BUCKET}/${ANYSCALE_PRESTART_DATA_PATH_TAR}"
done
for PY_VERSION_CODE in "${PY_VERSION_CODES[@]}"; do
    # Must keep this consistent with the image.
    ANYSCALE_PRESTART_DATA_PATH_TARGZ="${S3_PATH_PREFIX}/ray-opt-${PY_VERSION_CODE}-min.tar.gz"
    ANYSCALE_PRESTART_DATA_PATH_TAR="${S3_PATH_PREFIX}/ray-opt-${PY_VERSION_CODE}-min.tar"

    aws s3 cp "${TMP}/ray-${PY_VERSION_CODE}-cpu-min.tar.gz" \
        "s3://${ORG_DATA_BUCKET}/${ANYSCALE_PRESTART_DATA_PATH_TARGZ}"
    aws s3 cp "${TMP}/ray-${PY_VERSION_CODE}-cpu-min.tar" \
        "s3://${ORG_DATA_BUCKET}/${ANYSCALE_PRESTART_DATA_PATH_TAR}"
done
for WHEEL_PY_VERSION_CODE in "${WHEEL_PY_VERSION_CODES[@]}"; do
    WHEEL_FILE="ray-${RAY_VERSION}-${WHEEL_PY_VERSION_CODE}-manylinux2014_${HOSTTYPE}.whl"
    aws s3 cp "${TMP}/${WHEEL_FILE}" "s3://${ORG_DATA_BUCKET}/${S3_PATH_PREFIX}/${WHEEL_FILE}"
done

# Cleanup temp dir.
rm -rf "${TMP}"
