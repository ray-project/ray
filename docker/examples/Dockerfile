# The examples Docker image adds dependencies needed to run the examples

FROM rayproject/ray:latest

# Needed to run Tune example with a 'plot' call - which does not actually render a plot, but throws an error.
RUN apt-get update && apt-get install -y zlib1g-dev libgl1-mesa-dev libgtk2.0-dev && apt-get clean
RUN pip install --no-cache-dir -U pip \
    autorom[accept-rom-license] \
    gymnasium[atari] \
    scikit-image \
    tensorflow \
    lz4 \
    pytest-timeout \
    smart_open \
    tensorflow_probability \
    dm_tree \
    h5py   # Mutes FutureWarnings \
    bayesian-optimization \
    hyperopt \
    ConfigSpace==0.4.10 \
    sigopt \ 
    nevergrad \
    scikit-optimize \
    hpbandster \
    lightgbm \
    xgboost \
    torch \
    torchvision \
    tensorboardX \
    dragonfly-opt \
    zoopt \
    tabulate \ 
    mlflow \
    pytest-remotedata>=0.3.1 \
    matplotlib \
    jupyter \
    pandas
