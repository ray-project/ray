#!/bin/bash

set -euo pipefail

if [[ "${BUILDKITE:-}" == "" ]]; then
	echo "Only suppose to run on Buildkite" >/dev/stderr
	exit 1
fi

source anyscale/ci/setup-env.sh

echo "--- setup go compiler"
mkdir ~/goroot
curl -sfL https://go.dev/dl/go1.22.5.linux-amd64.tar.gz | tar -C ~/goroot -xzf -

export GOROOT="${HOME}/goroot/go"
export GOPATH="${HOME}/go"
export GO111MODULE=on
export PATH="${GOROOT}/bin:${PATH}"

(
    cd anyscale/go
    echo "--- go test"
    go test ./...

    echo "--- go mod tidy"
    go mod tidy
    git diff --exit-code

    echo "--- go fmt"
    go fmt ./...
    git diff --exit-code

    echo "--- build download_anyscale_data"
    CGO_ENABLED=0 go build -trimpath -buildvcs=false \
        -o /tmp/download_anyscale_data \
        ./download_anyscale_data
)

echo "--- save download_anyscale_data"
aws s3 cp /tmp/download_anyscale_data "${S3_TEMP}/download_anyscale_data"
