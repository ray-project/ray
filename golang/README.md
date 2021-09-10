# build ray
https://docs.ray.io/en/master/development.html#building-ray-on-linux-macos-full

cd $ray_source_dir/python

pip install -e . --verbose

# build golang shared library
If go_worker.cpp is changed, we need to rebuild shared library. Then copy the bazel-bin/libcore_worker_library_go.so to pkg/ray/packaged/lib/$GOOS-$GOARCH/. 

bazel build //:go_library

# start ray

ray start --head --port=6379

# copy proto generated file if proto changed

bazel build -s --sandbox_debug  //golang:cp_go_proto

# build ray golang worker (in golang directory)

go build -x -o worker example/worker/main.go

# test driver (in golang directory)
cd $ray_source_dir/golang

go run example/driver/test.go