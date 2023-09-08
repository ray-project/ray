import subprocess
import sys

if __name__ == "__main__":
    args = [sys.executable, "-m", "grpc_tools.protoc"]
    args.extend(sys.argv[1:])
    subprocess.check_call(args, stdout=sys.stdout, stderr=sys.stderr)
