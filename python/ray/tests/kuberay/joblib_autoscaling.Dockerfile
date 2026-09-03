ARG RAY_BASE_IMAGE
FROM ${RAY_BASE_IMAGE}

# Joblib is an optional integration and is therefore not present in the regular
# Ray image used by the KubeRay tests. Match Ray's test dependency pin.
COPY python/requirements/test-requirements.txt /tmp/ray-test-requirements.txt
RUN grep '^joblib==' /tmp/ray-test-requirements.txt \
        > /tmp/joblib-requirement.txt \
    && test "$(wc -l < /tmp/joblib-requirement.txt)" -eq 1 \
    && python -m pip install --no-cache-dir \
        -r /tmp/joblib-requirement.txt
