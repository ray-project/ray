go build -o worker pkg/worker/main.go

#test
LD_LIBRARY_PATH=/root/ray/bazel-bin/:$LD_LIBRARY_PATH
go run pkg/driver/test.go
