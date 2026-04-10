ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build-py$PYTHON
FROM $DOCKER_IMAGE_BASE_BUILD

SHELL ["/bin/bash", "-ice"]

COPY . .

RUN DOC_TESTING=1 ./ci/env/install-dependencies.sh && \
    # Pin opentelemetry packages to consistent versions to avoid
    # PROMETHEUS_HTTP_TEXT_METRIC_EXPORTER AttributeError in the dashboard
    pip install opentelemetry-api==1.39.0 opentelemetry-sdk==1.39.0 opentelemetry-exporter-prometheus==0.60b0 opentelemetry-semantic-conventions==0.60b0 opentelemetry-instrumentation-fastapi==0.60b0
