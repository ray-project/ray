import click
import logger
import subprocess
from typing import List

@click.command()
@click.argument("test_name", required=True, type=str)
@click.argument("passing_commit", required=True, type=str)
@click.argument("failing_commit", required=True, type=str)

def _get_commit_lists(passing_commit: str, failing_commit: str) -> List[str]:
    try 
        subprocess.check_output(
        )
    except Exception as e:
        logger.info(f'Failed to get commit list: {e}')

def main(test_name: str, passing_commit: str, failing_commit: str) -> None:
    return None

if __name__ == "__main__":
    main()
