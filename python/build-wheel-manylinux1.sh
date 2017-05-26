#!/bin/bash

cat << EOF > "/usr/bin/nproc"
#!/bin/bash
echo 1
EOF
chmod +x /usr/bin/nproc

/opt/python/cp35-cp35m/bin/pip install cmake
ln -s /opt/python/cp35-cp35m/bin/cmake /usr/bin/cmake

mkdir .whl
for PYTHON in cp27-cp27mu cp33-cp33m cp34-cp34m cp35-cp35m cp36-cp36m; do
  rm -rf *
  git checkout *
  pushd python
  /opt/python/${PYTHON}/bin/pip install numpy
  PATH=/opt/python/${PYTHON}/bin:$PATH /opt/python/${PYTHON}/bin/python setup.py bdist_wheel
  # In the future, run auditwheel here.
  mv dist/*.whl ../.whl/
  popd
done
