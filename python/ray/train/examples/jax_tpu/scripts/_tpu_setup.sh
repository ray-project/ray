#! /bin/bash
apt update
apt -y install apache2
pip3 install --upgrade pip
pip3 install --upgrade "jax[tpu]>=0.2.16" -f https://storage.googleapis.com/jax-releases/libtpu_releases.html
pip3 install --upgrade fabric dataclasses optax==0.0.6 git+https://github.com/deepmind/dm-haiku tensorboardX boto3 matplotlib clu einops 
python3 -c "import jax; jax.device_count(); jax.numpy.add(1, 1)"  # test if Jax has been installed correctly
pip3 install -U ray
sudo ray install-nightly
pip3 uninstall tensorflow -y 
pip3 install tensorflow --upgrade
pip3 install scipy --upgrade
pip3 install pandas --upgrade
pip3 install flax
