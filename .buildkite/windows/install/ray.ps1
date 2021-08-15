python -m pip install --upgrade pip
pip install -r reqs.txt
git clone -c core.symlinks=true https://github.com/ray-project/ray.git
cd ray
bash -c "certutil -generateSSTFromWU roots.sst && certutil -addstore -f root roots.sst && rm roots.sst"
echo shortname
fsutil 8dot3name query
bash -c ". ./ci/travis/ci.sh init"
bash -c ". ./ci/travis/ci.sh build"
bash -c ". ./ci/travis/ci.sh test_core"
echo shortname
fsutil 8dot3name query
bash -c ". ./ci/travis/ci.sh test_python"

#pip install -e . --verbose
