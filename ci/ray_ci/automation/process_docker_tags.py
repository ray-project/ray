import click

from ci.ray_ci.automation.docker_tags_lib import (
    backup_release_tags,
    delete_old_commit_tags,
)


@click.command()
@click.option(
    "--task",
    help="Task to perform",
    required=True,
    type=click.Choice(["backup_release_tags", "delete_commit_tags"]),
)
@click.option("--namespace", help="Docker namespace", required=True, type=str)
@click.option("--repository", help="Docker repository", required=True, type=str)
@click.option(
    "--release_versions", help="Release versions to backup", required=False, type=list
)
@click.option("--aws_ecr_repo", help="AWS ECR repository", required=False, type=str)
@click.option("--n_days", help="Days to keep commit tags", required=False, type=int)
@click.option("--num_tags", help="Number of tags to process", required=False, type=int)
def main(task, namespace, repository, release_versions, aws_ecr_repo, n_days, num_tags):
    if task == "backup_release_tags":
        backup_release_tags(namespace, repository, release_versions, aws_ecr_repo, num_tags)
    elif task == "delete_commit_tags":
        delete_old_commit_tags(namespace, repository, n_days, num_tags)
    else:
        raise ValueError(
            "Invalid task choice."
            "Choose from 'backup_release_tags' or 'delete_commit_tags'"
        )


if __name__ == "__main__":
    main()
