# The examples Docker image adds dependencies needed to run the examples

FROM ray-project/deploy

# This updates numpy to 1.14 and mutes errors from other libraries
RUN conda install -y numpy
# Needed to run Tune example with a 'plot' call - which does not actually render a plot, but throws an error.
RUN apt-get install -y zlib1g-dev libgl1-mesa-dev
RUN pip install -U pip
RUN pip install gym[atari] opencv-python-headless tensorflow lz4 pytest-timeout smart_open tensorflow_probability dm_tree
RUN pip install -U h5py  # Mutes FutureWarnings
RUN pip install --upgrade bayesian-optimization hyperopt
RUN pip install ConfigSpace==0.4.10
RUN pip install --upgrade sigopt nevergrad scikit-optimize hpbandster lightgbm xgboost torch torchvision tensorboardX dragonfly-opt zoopt
RUN pip install -U tabulate mlflow
RUN pip install -U pytest-remotedata>=0.3.1
RUN pip install -U matplotlib jupyter pandas
