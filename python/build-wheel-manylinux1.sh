#!/bin/bash

cat << EOF > "/usr/bin/nproc"
#!/bin/bash
echo 1
EOF
chmod +x /usr/bin/nproc

mkdir .whl
for PYTHON in cp27-cp27mu cp33-cp33m cp34-cp34m cp35-cp35m cp36-cp36m; do
  rm -rf *
  git checkout *
  pushd python
  /opt/python/${PYTHON}/bin/pip install numpy
  PATH=/opt/python/${PYTHON}/bin:$PATH /opt/python/${PYTHON}/bin/python setup.py bdist_wheel
  # For some reason, this needs to be run twice to cause binaries to appear in
  # the .whl file.
  PATH=/opt/python/${PYTHON}/bin:$PATH /opt/python/${PYTHON}/bin/python setup.py bdist_wheel
  # In the future, run auditwheel here.
  mv dist/*.whl ../.whl/
  popd
done
