# build ray
https://docs.ray.io/en/master/development.html#building-ray-on-linux-macos-full

cd $raySourceDir/python

pip install -e . --verbose

# build golang shared library
cd $raySourceDir

bazel build //:libcore_worker_library_go.so

# start ray
cd $raySourceDir

export LD_LIBRARY_PATH=${PWD}/bazel-bin/:$LD_LIBRARY_PATH

ray start --head --port=6379

#build ray golang worker (in golang directory)
cd $raySourceDir/golang

go build -o worker pkg/worker/main.go

#test driver (in golang directory)
cd $raySourceDir/golang

go run pkg/driver/test.go
