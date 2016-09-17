# Bulding on top of deploy image, this Dockerfile adds libraries needed
# for running examples.

FROM ray-project/ray:deploy

# Tensorflow
RUN pip install --upgrade https://storage.googleapis.com/tensorflow/linux/cpu/tensorflow-0.9.0-cp27-none-linux_x86_64.whl

# SciPy
RUN pip install scipy

# Gym
RUN sudo apt-get -y install zlib1g-dev libjpeg-dev xvfb libav-tools xorg-dev python-opengl libsdl2-dev swig wget
RUN pip install gym[atari]
