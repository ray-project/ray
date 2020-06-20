Install Ray for Java
====================

Ray works for Java 8 and above. Currently, we only support building Ray from source.

Build from source
-----------------

Install dependencies
^^^^^^^^^^^^^^^^^^^^

First, make sure JDK 8 or above is installed on your machine. If not, you can download it from `here <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`_.

Then install the following dependencies.

For Ubuntu users, run the following commands:
::

  sudo apt-get update
  sudo apt-get install -y maven build-essential curl unzip psmisc python # we install python here because python2 is required to build the webui

  pip install cython==0.29.0

For macOS users, run the following commands:
::

  brew update
  brew install maven wget

  pip install cython==0.29.0

Build Ray
^^^^^^^^^

Then we can start building Ray with the following commands:
::

  git clone https://github.com/ray-project/ray.git
  cd ray

  # Install Bazel.
  ci/travis/install-bazel.sh

  # build native components
  ./build.sh -l java

  # build java API
  cd java
  mvn clean install -Dmaven.test.skip

Run tests
^^^^^^^^^
::

  # in `ray/java` directory
  mvn test
