#!/bin/bash

set -euo pipefail

FOSSA_BIN="$HOME/fossa/fossa"
OUTPUT_FOLDER="$HOME/fossa_output_folder"

FOSSA_API_KEY="$(
    aws secretsmanager get-secret-value --region us-west-2 \
    --secret-id oss-ci/fossa-api-key \
    --query SecretString --output text
)"
export FOSSA_API_KEY

mkdir -p "$OUTPUT_FOLDER"

source "$HOME/venv/bin/activate"
python ci/fossa/ray_oss_analysis.py -cmd bazelisk -p //:gen_ray_pkg -o "$OUTPUT_FOLDER" --log-file "$OUTPUT_FOLDER/package_license_analysis.log"
cd "$OUTPUT_FOLDER"; "$FOSSA_BIN" analyze -p ray --fossa-deps-file fossa_deps.yaml

# copy to artifacts folder for upload
cp "$OUTPUT_FOLDER"/askalono_results.json /artifact-mount/
cp "$OUTPUT_FOLDER"/package_license_analysis.log /artifact-mount/
cp "$OUTPUT_FOLDER"/askalono_results.xlsx /artifact-mount/
cp "$OUTPUT_FOLDER"/fossa_deps.yaml /artifact-mount/
