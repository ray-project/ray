#!/bin/bash

# Don't use nbconvert or jupytext unless you're willing
# to check each subprocess unit and validate that errors
# aren't being consumed/hidden.

set -euxo pipefail

# Use the AWS CLI to fetch BRAVE_API_KEY from Secrets Manager.
# Replace 'my-brave-api-key-secret' with the actual secret name.
BRAVE_API_KEY=$(aws secretsmanager get-secret-value \
  --secret-id brave-search-api-key \
  --query SecretString \
  --output text)

export BRAVE_API_KEY

for nb in \
  "01 Deploy_custom_mcp_in_streamable_http_with_ray_serve" \
  "02 Build_mcp_gateway_with_existing_ray_serve_apps" \
  "03 Deploy_single_mcp_stdio_docker_image_with_ray_serve" \
  "04 Deploy_multiple_mcp_stdio_docker_images_with_ray_serve" \
  "05 (Optional) Build_docker_image_for_mcp_server"
do
  python ci/nb2py.py "${nb}.ipynb" "${nb}.py" --ignore-cmds
  python "${nb}.py"
  rm "${nb}.py"
done
