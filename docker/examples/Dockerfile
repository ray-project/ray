# The examples Docker image adds dependencies needed to run the examples

FROM ray-project/deploy
RUN conda install -y -c conda-forge tensorflow
RUN apt-get install -y zlib1g-dev
RUN pip install gym[atari]==0.9.5 opencv-python==3.2.0.8 smart_open
# RUN conda install -y -q pytorch torchvision -c soumith
