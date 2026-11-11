# syntax=docker/dockerfile:1.3-labs
#
# Installs the Ray wheel into a ray-llm-vllm image (docker/ray-llm-vllm), for
# both the CI image and the release test image.
#
ARG BASE_IMAGE
ARG RAY_WHEEL_IMAGE

FROM ${RAY_WHEEL_IMAGE} AS wheel-source
FROM ${BASE_IMAGE}

ARG RAY_COMMIT=unknown-commit
ARG RAY_VERSION=3.0.0.dev0

LABEL io.ray.ray-commit="${RAY_COMMIT}"
LABEL io.ray.ray-version="${RAY_VERSION}"

# Back to vllm/vllm-openai's value: the final image only holds NCCL for uv.
ENV UV_OVERRIDE=/etc/uv-overrides.txt

COPY --from=wheel-source /opt/artifacts/*.whl /home/ray/

RUN <<EOF
#!/bin/bash
set -euo pipefail

WHEEL_FILES=(/home/ray/ray-*.whl)
if [[ ${#WHEEL_FILES[@]} -ne 1 ]]; then
    echo "Error: Expected 1 ray wheel file, but found ${#WHEEL_FILES[@]} in /home/ray/." >&2
    ls -l /home/ray/*.whl >&2
    exit 1
fi

# Constraints rather than overrides: if Ray needs a version of a package that
# differs from vLLM's, fail here so Ray's requirements get adjusted.
uv pip install --system --no-cache --constraint /home/ray/vllm-constraints.txt \
    "${WHEEL_FILES[0]}[serve,llm]"

pip freeze > /home/ray/pip-freeze.txt

echo "Ray version: $(python -c 'import ray; print(ray.__version__)')"
EOF

CMD ["/bin/bash"]
