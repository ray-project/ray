import argparse
from hashlib import md5


def md5sum(filename, buf_size=8192):
    m = md5()
    with open(filename, "rb") as f:
        data = f.read(buf_size)
        while data:
            m.update(data)
            data = f.read(buf_size)
    return m.hexdigest()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("A simple cross-platform md5sum."))
    parser.add_argument(
        "files", type=str, nargs="+", help="The checksum files.")
    args = parser.parse_args()
    for f in args.files:
        print(f"{md5sum(f)}  {f}")
