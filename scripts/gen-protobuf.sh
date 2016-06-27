DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# TODO(mehrdad): How would this look in windows, where does the protoc executable go?
# On Linux, we compile it ourselves, on Windows we might not want to do that (?)
mkdir -p $DIR/../lib/python/ray/internal/
$DIR/../thirdparty/grpc/bins/opt/protobuf/protoc -I ../protos/ --python_out=$DIR/../lib/python/ray/internal/ ../protos/graph.proto
$DIR/../thirdparty/grpc/bins/opt/protobuf/protoc -I ../protos/ --python_out=$DIR/../lib/python/ray/internal/ ../protos/types.proto
