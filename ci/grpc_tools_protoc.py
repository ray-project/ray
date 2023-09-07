import sys

if __name__ == "__main__":
    import grpc_tools.protoc

    sys.exit(grpc_tools.protoc.main(sys.argv[1:]))
