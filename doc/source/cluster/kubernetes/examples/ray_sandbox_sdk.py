"""Run the Modal SDK example against a Ray Sandbox facade."""

import os

import modal


def main():
    url = os.environ.get("RAY_SANDBOX_GRPC_URL", "http://127.0.0.1:50051")
    with modal.Client.anonymous(url) as client:
        app = modal.App.lookup(
            "kuberay-sandbox-example", client=client, create_if_missing=True
        )
        sandbox = modal.Sandbox.create(
            app=app,
            client=client,
            image=modal.Image.from_registry("docker.io/library/python:3.12-slim"),
            cpu=0.25,
            memory=256,
            timeout=300,
            workdir="/workspace",
            block_network=True,
        )
        try:
            sandbox.filesystem.write_text(
                'print("Hello from a Ray sandbox on KubeRay!")\n',
                "/workspace/main.py",
            )
            process = sandbox.exec("python3", "/workspace/main.py")
            stdout = process.stdout.read()
            stderr = process.stderr.read()
            process.wait()
            print(stdout, end="")
            if process.returncode != 0:
                raise RuntimeError(f"Sandbox command failed: {stderr}")
            assert stdout == "Hello from a Ray sandbox on KubeRay!\n"

            sandbox.filesystem.write_text(stdout, "/workspace/result.txt")
            assert sandbox.filesystem.read_text("/workspace/result.txt") == stdout
            print("File round trip succeeded.")
        finally:
            sandbox.terminate()
            print("Sandbox terminated.")


if __name__ == "__main__":
    main()
