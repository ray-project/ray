import json
import sys


def main():
    argv_file = sys.argv[1]
    with open(argv_file, "wt") as fp:
        json.dump(sys.argv, fp)

    sys.exit(0)


if __name__ == "__main__":
    main()
