# For running the python tests

protoc -I ../protos/ --python_out=. --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_python_plugin` ../protos/ray.proto
protoc -I ../protos/ --python_out=. --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_python_plugin` ../protos/types.proto
