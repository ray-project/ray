# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

COPY python/requirements_*.txt ./

RUN <<EOF
#!/bin/bash

PYTHON_CODE="$(python -c "import sys; v=sys.version_info; print(f'py{v.major}{v.minor}')")"

if [[ "${PYTHON_CODE}" != "py311" ]]; then
    echo "ray-llm only support Python 3.11 now (this image is for ${PYTHON_CODE})."
    exit 1
fi

pip install --no-deps -r "python/requirements_compiled_rayllm_py${PYTHON_CODE}.txt"

# Export installed packages
$HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt

EOF
