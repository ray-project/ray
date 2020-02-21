# The examples Docker image adds dependencies needed to run the examples

FROM ray-project/deploy

# This updates numpy to 1.14 and mutes errors from other libraries
RUN pip install numpy==1.18.0
# Needed to run Tune example with a 'plot' call - which does not actually render a plot, but throws an error.
RUN apt-get install -y zlib1g-dev libgl1-mesa-dev
RUN pip install gym[atari] opencv-python-headless tensorflow==1.15.0 lz4 keras pytest-timeout smart_open
RUN pip install -U h5py  # Mutes FutureWarnings
RUN pip install --upgrade bayesian-optimization
RUN pip install --upgrade hyperopt==0.1.2
RUN pip install --upgrade sigopt
# RUN pip install --upgrade nevergrad
RUN pip install --upgrade scikit-optimize
RUN conda install -y pytorch-cpu torchvision-cpu -c pytorch
