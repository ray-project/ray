ARG RAYCI_GLOBAL_CONFIG="ci/ray_ci/runtime_config.yaml"

FROM python:3.9

ENV RAYCI_GLOBAL_CONFIG=RAYCI_GLOBAL_CONFIG

RUN mkdir -p /rayci
WORKDIR /rayci
COPY . .
ENTRYPOINT ["./run"]
