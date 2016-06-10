sudo apt-get update
sudo apt-get install -y git cmake build-essential autoconf libtool python-dev python-numpy python-pip libboost-all-dev unzip
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
cd ../lib/python
sudo python setup.py install
