ARG DOCKER_IMAGE_BASE_TEST
FROM $DOCKER_IMAGE_BASE_TEST

ENV RAY_INSTALL_JAVA=1

RUN apt-get install -y -qq \
    maven openjdk-8-jre openjdk-8-jdk

# Move out of working dir /ray
# Delete stale data
WORKDIR /
RUN rm -rf /ray

RUN mkdir /ray
WORKDIR /ray

# Copy new ray files
COPY . .

RUN RLLIB_TESTING=1 TRAIN_TESTING=1 TUNE_TESTING=1 ./ci/env/install-dependencies.sh
