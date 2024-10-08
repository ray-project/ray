CMake build of gRPC doesn't support this opencensus thing.
https://github.com/grpc/grpc/issues/20212

We could, for sure, patch gRPC but for one deprecated package with too many problems - it's too much.
Instead, we decided to include a single header and it seems to be enough.
We don't have to update it often, as the last update happened more than a year ago.
https://github.com/eugo-inc/grpc/blob/96d0f84d920de6ca139889616c97c69b23a9455a/include/grpcpp/opencensus.h#L4
