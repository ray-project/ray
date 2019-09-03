# The examples Docker image adds dependencies needed to run the examples

FROM ray-project/deploy

# This updates numpy to 1.14 and mutes errors from other libraries
RUN conda install -y numpy
RUN apt-get install -y zlib1g-dev
# The following is needed to support TensorFlow 1.14
RUN conda remove -y --force wrapt
RUN pip install gym[atari] opencv-python-headless tensorflow lz4 keras pytest-timeout smart_open tensorflow_probability
RUN pip install -U h5py  # Mutes FutureWarnings
RUN pip install --upgrade bayesian-optimization
RUN pip install --upgrade git+git://github.com/hyperopt/hyperopt.git
RUN pip install --upgrade sigopt nevergrad scikit-optimize hpbandster ConfigSpace lightgbm xgboost
RUN pip install -U mlflow
RUN pip install -U pytest-remotedata>=0.3.1
RUN conda install pytorch-cpu torchvision-cpu -c pytorch
