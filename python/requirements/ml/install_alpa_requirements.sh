sudo rm -rf /var/lib/apt/lists/lock
sudo rm -rf /var/cache/apt/archives/lock
sudo rm -rf /var/lib/dpkg/lock*
sudo dpkg --configure -a
sudo apt update
sudo apt install coinor-cbc -y
sudo add-apt-repository ppa:ubuntu-toolchain-r/test -y
sudo apt-get update 
sudo apt-get install --only-upgrade libstdc++6 -y
pip install cupy-cuda113==10.6.0
python -m cupyx.tools.install_library --cuda 11.3 --library nccl
python -c "from cupy.cuda import nccl"