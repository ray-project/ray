import os
import sys

import click


@click.command()
@click.argument("state_file", type=str)
@click.argument("exit_1", type=int)
@click.argument("exit_2", type=int)
@click.argument("exit_3", type=int)
def main(
    state_file: str,
    exit_1: int,
    exit_2: int,
    exit_3: int,
):
    if not os.path.exists(state_file):
        state = 0
    else:
        with open(state_file, "rt") as fp:
            state = int(fp.read())

    state += 1

    with open(state_file, "wt") as fp:
        fp.write(str(state))

    if state == 1:
        print(f"Exiting with status: {exit_1}")
        sys.exit(exit_1)

    if state == 2:
        print(f"Exiting with status: {exit_2}")
        sys.exit(exit_2)

    if state == 3:
        print(f"Exiting with status: {exit_3}")
        sys.exit(exit_3)


if __name__ == "__main__":
    main()
