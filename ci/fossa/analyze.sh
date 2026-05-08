#!/bin/bash

set -euo pipefail

OUTPUT_FOLDER="$HOME/fossa_output_folder"
mkdir -p "$OUTPUT_FOLDER"

FOSSA_API_KEY="$(
    aws secretsmanager get-secret-value --region us-west-2 \
    --secret-id oss-ci/fossa-api-key \
    --query SecretString --output text
)"
export FOSSA_API_KEY

# run Full fossa analyze on entire ray repository
fossa analyze

# run ray_oss_analysis to get askalono results and generate fossa deps file
bazel run //ci/fossa:ray_oss_analysis -- -cmd bazelisk -p //:gen_ray_pkg -o "$OUTPUT_FOLDER" --log-file "$OUTPUT_FOLDER/package_license_analysis.log"
cd "$OUTPUT_FOLDER"; fossa analyze -p ray --fossa-deps-file fossa_deps.yaml

# copy to artifacts folder for upload
cp "$OUTPUT_FOLDER"/askalono_results.json /artifact-mount/
cp "$OUTPUT_FOLDER"/package_license_analysis.log /artifact-mount/
cp "$OUTPUT_FOLDER"/fossa_deps.yaml /artifact-mount/
