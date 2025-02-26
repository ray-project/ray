ARG DOCKER_IMAGE_BASE_BUILD
FROM $DOCKER_IMAGE_BASE_BUILD

ENV NO_INSTALL=1
ENV RAY_TEST_SCRIPT=./run_release_test
ENV NO_ARTIFACTS=1

RUN mkdir -p /rayci
RUN pip install --upgrade pip
RUN pip install anyscale==0.26.1
WORKDIR /rayci
COPY . .
ENTRYPOINT ["bash", "./run"]
