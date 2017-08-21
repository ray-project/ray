#!/bin/bash

cat << EOF > "/usr/bin/nproc"
#!/bin/bash
echo 10
EOF
chmod +x /usr/bin/nproc

mkdir .whl
for PYTHON in cp27-cp27mu cp33-cp33m cp34-cp34m cp35-cp35m cp36-cp36m; do
  # The -f flag is passed twice to also run git clean in the arrow subdirectory.
  # The -d flag removes directories. The -x flag ignores the .gitignore file,
  # and the -e flag ensures that we don't remove the .whl directory.
  git clean -f -f -x -d -e .whl
  pushd python
    # Fix the numpy version because this will be the oldest numpy version we can
    # support.
    /opt/python/${PYTHON}/bin/pip install numpy==1.10.4 cython
    PATH=/opt/python/${PYTHON}/bin:$PATH /opt/python/${PYTHON}/bin/python setup.py bdist_wheel
    # In the future, run auditwheel here.
    mv dist/*.whl ../.whl/
  popd
done

# Rename the wheels so that they can be uploaded to PyPI. TODO(rkn): This is a
# hack, we should use auditwheel instead.
pushd .whl
  find *.whl -exec bash -c 'mv $1 ${1//linux/manylinux1}' bash {} \;
popd
