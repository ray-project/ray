import os
import sys

import click

from ray_release.result import Result, should_retry, write_retry_marker


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

    exit_codes = {1: exit_1, 2: exit_2, 3: exit_3}
    if state not in exit_codes:
        return

    exit_code = exit_codes[state]
    print(f"Exiting with status: {exit_code}")

    # Same handshake as scripts/run_release_test.py, using the same logic.
    result = Result(return_code=exit_code, runtime=0)
    result.will_retry = should_retry(result)
    write_retry_marker(result)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
