python -m pip install --upgrade pip
pip install -r reqs.txt
git clone -c core.symlinks=true https://github.com/ray-project/ray.git
cd ray
bash -c "certutil -generateSSTFromWU roots.sst && certutil -addstore -f root roots.sst && rm roots.sst"
bash -c ". ./ci/travis/ci.sh init"
bash -c ". ./ci/travis/ci.sh build"
#copy necessary java includes
cp -r c:\openjdk-16\include bazel-ray\external\local_jdk\
bash -c ". ./ci/travis/ci.sh build"
bash -c ". ./ci/travis/ci.sh test_core"
bash -c ". ./ci/travis/ci.sh test_python"

#pip install -e . --verbose
