# Bulding on top of base test image, this Dockerfile adds libraries
# needed for running additional examples.

FROM ray-project/ray:test-base

# Tensorflow
RUN pip install --upgrade https://storage.googleapis.com/tensorflow/linux/cpu/tensorflow-0.9.0-cp27-none-linux_x86_64.whl

# SciPy
RUN pip install scipy
