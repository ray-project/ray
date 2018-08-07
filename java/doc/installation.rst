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
  sudo apt-get install -y maven cmake pkg-config build-essential autoconf curl libtool unzip flex bison psmisc python # we install python here because python2 is required to build the webui

  # If you are not using Anaconda, you need the following.
  sudo apt-get install python-dev  # For Python 2.
  sudo apt-get install python3-dev  # For Python 3.

  # If you are on Ubuntu 14.04, you need the following.
  pip install cmake

  pip install cython

For macOS users, run the following commands:
::

  brew update
  brew install maven cmake pkg-config automake autoconf libtool openssl bison wget

  pip install cython

Build Ray
^^^^^^^^^

Then we can start building Ray with the following commands:
::

  git clone https://github.com/ray-project/ray.git
  cd ray

  # build native components
  ./build.sh -l java

  # build java API
  cd java
  mvn clean install -Dmaven.test.skip

Run tests
^^^^^^^^^
::

  # in `ray/java` directory
  export RAY_CONFIG=ray.config.ini
  mvn test
