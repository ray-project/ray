#!/bin/bash

set -euo pipefail

source anyscale/ci/setup-env.sh

if [[ "${RAY_RELEASE_BUILD}" != "true" ]]; then
    # Cleanup temp dir.
    echo "Not a release build, skipping upload the site package."
    exit
fi

DEPLOY_ENVIRONMENT="${1:-development}"

PY_VERSION_CODES=(py38 py39 py310 py311)

TMP="$(mktemp -d)"

if [[ "${DEPLOY_ENVIRONMENT}" == "staging" ]]; then
    readonly ORG_DATA_BUCKET=anyscale-staging-organization-data-us-west-2
    readonly DEPLOY_ROLE="arn:aws:iam::623395924981:role/buildkite-deploy-to-staging"
else
    readonly ORG_DATA_BUCKET=anyscale-dev-organization-data-us-west-2
    readonly DEPLOY_ROLE="arn:aws:iam::830883877497:role/buildkite-deploy-to-premerge"
fi

echo "--- Upload to org data for ${DEPLOY_ENVIRONMENT}"

if [[ "${BUILDKITE:-}" == "true" ]]; then
    eval "$(aws sts assume-role \
        --role-arn "${DEPLOY_ROLE}" \
        --role-session-name "runtime-sitepkg-${RAYCI_BUILD_ID}" \
        --duration-seconds 900 | python anyscale/ci/assume_role_envs.py)"
fi

RAY_COMMIT="$(git rev-parse HEAD)"

for PY_VERSION_CODE in "${PY_VERSION_CODES[@]}"; do
    # All tar.gz's are the same, we only need to upload one of them.
    # so just upload the basic ray:cpu one.

    aws s3 cp "${S3_TEMP}/ray-opt/${PY_VERSION_CODE}/ray-${PY_VERSION_CODE}-cpu.tar.gz" \
        "${TMP}/ray-${PY_VERSION_CODE}-cpu.tar.gz"

    aws s3 cp "${TMP}/ray-${PY_VERSION_CODE}-cpu.tar.gz" \
        "s3://${ORG_DATA_BUCKET}/common/ray-opt/${RAY_VERSION}/${RAY_COMMIT}/ray-opt-${PY_VERSION_CODE}.tar.gz"
done

# Cleanup temp dir.
rm -rf "${TMP}"
