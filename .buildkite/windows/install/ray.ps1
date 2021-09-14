python -m pip install --upgrade pip
pip install -r reqs.txt
echo started
git clone --single-branch --branch windows-dockerfile-v3 -c core.symlinks=true https://github.com/ellimac54/ray.git
cd ray
bash -c "certutil -generateSSTFromWU roots.sst && certutil -addstore -f root roots.sst && rm roots.sst"
bash -c ". ./ci/travis/ci.sh init"
bash -c ". ./ci/travis/ci.sh build"
bash -c ". ./ci/travis/ci.sh test_core"
bash -c ". ./ci/travis/ci.sh test_python"
