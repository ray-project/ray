ARG DOCKER_IMAGE_BASE_BUILD
FROM $DOCKER_IMAGE_BASE_BUILD

ENV RAYCI_GLOBAL_CONFIG="ci/ray_ci/runtime_config.yaml"

RUN mkdir -p /rayci
WORKDIR /rayci
COPY . .
ENTRYPOINT ["./ci_pipeline_scheduler"]
