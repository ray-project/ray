import subprocess

RAY_CHECKOUT_DIR = "/tmp/ray-checkout"


def build():
    process = subprocess.Popen(
        ["bazel", "build", "-c", "fastbuild", "//:ray_pkg"],
        stdout=subprocess.PIPE,
        cwd=RAY_CHECKOUT_DIR,
    )
    for line in process.stdout:
        print(line.decode().strip())
    process.wait()


def main():
    build()


if __name__ == "__main__":
    main()
