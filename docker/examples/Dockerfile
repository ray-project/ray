# The examples Docker image adds dependencies needed to run the examples

FROM ray-project/deploy

# This updates numpy to 1.14 and mutes errors from other libraries
RUN conda install -y numpy
RUN apt-get install -y zlib1g-dev
RUN pip install gym[atari] opencv-python==3.2.0.8 tensorflow lz4 keras
RUN pip install --upgrade git+git://github.com/hyperopt/hyperopt.git
RUN conda install pytorch-cpu torchvision-cpu -c pytorch
