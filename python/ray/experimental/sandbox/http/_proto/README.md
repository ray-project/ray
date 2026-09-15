# Vendored gRPC wire contract

This directory holds the wire contract the Ray Sandbox gRPC facade
(`ray/experimental/sandbox/http/grpc_facade.py`) implements, so the facade
builds and tests without any third-party client SDK installed.

- `sandbox_control.proto`, `sandbox_exec.proto`: hand-authored, minimal
  subsets of the external control-plane and command-router services. gRPC
  routes on the fully qualified method path and protobuf frames fields by
  number, so the package, service, method, and field identifiers reproduce
  the external service's names verbatim. **Do not renumber fields.**
- `*_pb2.py`: generated protobuf message stubs.
- `*_grpc.py`: generated grpclib service stubs.

The stubs are checked in, so only the `protobuf` and `grpclib` runtimes are
needed at build and test time. The top-level `.gitignore` ignores `*_pb2.py`;
use `git add -f` when adding a new stub.

## Regenerating

Compile from the repo's `python/` directory so the generated cross-imports
are package-qualified. The pinned `grpcio-tools` emits protobuf 4.25-era
gencode, which runs on every protobuf runtime Ray supports.

```bash
pip install "grpcio-tools==1.62.3" "grpclib==0.4.9"
cd python
python -m grpc_tools.protoc -I . --python_out=. --grpclib_python_out=. \
  ray/experimental/sandbox/http/_proto/sandbox_control.proto \
  ray/experimental/sandbox/http/_proto/sandbox_exec.proto
```
