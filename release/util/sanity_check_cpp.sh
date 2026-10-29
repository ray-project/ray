#!/usr/bin/env bash

# This script generate a ray C++ template and run example
set -e

# Token authentication is enabled by default for local clusters (#64755), but
# the C++ API only loads tokens from RAY_AUTH_TOKEN / RAY_AUTH_TOKEN_PATH — it
# has no fallback to the default ~/.ray/auth_token file that `ray start`
# writes. The example's auto-started cluster and its C++ driver therefore only
# agree on a token when both read it from the environment (the same approach
# the in-repo cpp bazel tests use). Without this, the driver fails with
# InvalidAuthToken, which is how the 2.59.0 release sanity run first caught it.
AUTH_TOKEN_FILE="$(mktemp)"
trap 'rm -f "${AUTH_TOKEN_FILE}"' EXIT
python -c "import secrets; print(secrets.token_hex(32), end='')" > "${AUTH_TOKEN_FILE}"
export RAY_AUTH_TOKEN_PATH="${AUTH_TOKEN_FILE}"

rm -rf ray-template
ray cpp --generate-bazel-project-template-to ray-template
(
    cd ray-template

    # Our generated CPP template does not work with bazel 7.x ,
    # so pin the bazel version to 6
    USE_BAZEL_VERSION=6.5.0 bash run.sh
)
