#!/bin/bash
set -euxo pipefail

# Build the dashboard using Node
cd "$(dirname "$0")/../../python/ray/dashboard/client"

# Clean previous builds (optional)
rm -rf build

# Install and build
npm ci
npm run build

# Archive the output to be used in the wheel build
tar -czf dashboard_build.tar.gz -C build .
mv dashboard_build.tar.gz ../../../../../dashboard_build.tar.gz
