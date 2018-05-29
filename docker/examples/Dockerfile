# The examples Docker image adds dependencies needed to run the examples

FROM ray-project/deploy
RUN apt-get install -y zlib1g-dev
RUN pip install gym[atari] opencv-python==3.2.0.8 tensorflow
RUN pip install --upgrade git+git://github.com/hyperopt/hyperopt.git
RUN conda install pytorch-cpu torchvision-cpu -c pytorch
