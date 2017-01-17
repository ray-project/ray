# The examples Docker image adds dependencies needed to run the examples

FROM ray-project/deploy
RUN conda install -y -c conda-forge tensorflow
RUN apt-get install -y zlib1g-dev
RUN pip install gym[atari]
