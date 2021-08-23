#

#build ray golang worker
go build -o worker pkg/worker/main.go

#test driver
LD_LIBRARY_PATH=/root/ray/bazel-bin/:$LD_LIBRARY_PATH
go run pkg/driver/test.go
