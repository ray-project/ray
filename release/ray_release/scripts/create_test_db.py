import sqlite3

import click
import boto3

DB_NAME = "ray_test.db"
AWS_BUCKET = "ray-release-automation-results"


@click.command()
@click.option(
    "--upload",
    is_flag=True,
    show_default=True,
    default=False,
    help=("Upload the computed coverage data to S3."),
)
def main(upload: bool) -> None:
    connect = sqlite3.connect(DB_NAME)
    db = connect.cursor()

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS tests (
          [id] INTEGER PRIMARY KEY AUTOINCREMENT,
          [name] TEXT NOT NULL,
          [state] TEXT NOT NULL,
          [oncall] TEXT NOT NULL
        )"""
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS test_results (
          [id] INTEGER PRIMARY KEY AUTOINCREMENT,
          [test_id] INTEGER NOT NULL,
          [status] TEXT NOT NULL,
          [revision] TEXT NOT NULL,
          [branch] TEXT NOT NULL,
          [timestamp] INTEGER NOT NULL,
          FOREIGN KEY(test_id) REFERENCES tests(id)
        )"""
    )

    db.execute(
        """
        CREATE INDEX IF NOT EXISTS test_result_timestamp_idx ON test_results(timestamp)
    """
    )

    connect.commit()

    if upload:
        boto3.client("s3").upload_file(
            DB_NAME,
            AWS_BUCKET,
            DB_NAME,
        )


if __name__ == "__main__":
    main()
