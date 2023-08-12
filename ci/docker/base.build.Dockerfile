ARG DOCKER_IMAGE_BASE_TEST=cr.ray.io/rayproject/oss-ci-base_test
FROM $DOCKER_IMAGE_BASE_TEST

ENV RAY_INSTALL_JAVA=1

RUN apt-get install -y -qq maven openjdk-8-jre openjdk-8-jdk

COPY . .

# init also calls install-dependencies.sh (again)
RUN BUILD=1 ./ci/ci.sh init
