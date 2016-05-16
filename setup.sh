sudo apt-get update
sudo apt-get install git cmake build-essential python-dev python-numpy automake autoconf libtool python-pip libboost-all-dev unzip
sudo pip install --ignore-installed six # getting rid of an old version of six, if it is installed (needed for Ubuntu 14.04)
sudo pip install -r requirements.txt
cd thirdparty
bash download_thirdparty.sh
bash build_thirdparty.sh
cd numbuf
cd python
sudo python setup.py install
mkdir -p ../../../build
cd ../../../build
cmake ..
sudo make install
cd ../lib/orchpy
sudo python setup.py install
