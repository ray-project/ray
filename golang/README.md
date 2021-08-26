# build ray
https://docs.ray.io/en/master/development.html#building-ray-on-linux-macos-full

cd $ray_source_dir/python

pip install -e . --verbose

# build golang shared library
If go_worker.cpp is changed, we need to rebuild shared library. Then copy the bazel-bin/libcore_worker_library_go.so to pkg/ray/packaged/lib/$GOOS-$GOARCH/. 

cd $ray_source_dir

bazel build //:libcore_worker_library_go.so

# start ray
cd $ray_source_dir

ray start --head --port=6379

# build ray golang worker (in golang directory)
cd $ray_source_dir/golang

go build -x -o worker pkg/worker/main.go

# test driver (in golang directory)
cd $ray_source_dir/golang

go run pkg/driver/test.go