#!/bin/bash

cat << EOF > "/usr/bin/nproc"
#!/bin/bash
echo 1
EOF
chmod +x /usr/bin/nproc

/opt/python/cp35-cp35m/bin/pip install cmake
ln -s /opt/python/cp35-cp35m/bin/cmake /usr/bin/cmake

/opt/python/cp27-cp27m/bin/pip install numpy
cd python
PYTHONPATH=build/bdist.linux-x86_64/wheel/:$PYTHONPATH PATH=/opt/python/cp27-cp27m/bin:$PATH /opt/python/cp27-cp27m/bin/python setup.py bdist_wheel
