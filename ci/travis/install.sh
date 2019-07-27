#!/bin/bash

if [[ $TRAVIS_OS_NAME == 'linux' ]]; then
  # Linux test uses Docker

  # We need to update the Docker version provided by Travis CI
  # in order to set shm size, a setting required by some of the
  # tests.
  sudo apt-get update
  sudo apt-get -y install apt-transport-https ca-certificates
  sudo apt-key adv --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys 58118E89F3A912897C070ADBF76221572C52609D
  echo deb https://apt.dockerproject.org/repo ubuntu-trusty main | sudo tee --append /etc/apt/sources.list.d/docker.list
  sudo apt-get update
  sudo apt-get -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" install docker-engine
  docker version

  # We tar the current checkout, then include it in the Docker image
  tar --exclude './docker' -c . > ./docker/test-base/ray.tar
  docker build --no-cache -t ray-project/ray:test-base docker/test-base
  rm ./docker/test-base/ray.tar
  docker build --no-cache -t ray-project/ray:test-examples docker/test-examples
  docker ps -a
else
  # Mac OS X test
  ./install-dependencies.sh
  ./setup.sh
  ./build.sh
fi
