import pickle
from sphinx.project import Project
import os
import time
from typing import List
from datetime import datetime
import click

PENDING_FILES_PATH = "pending_files.txt"
ENVIRONMENT_PICKLE = "_build/doctrees/environment.pickle"


def list_pending_files(ray_dir: str) -> List[str]:
    """List all files that are added/modified in git repo."""
    pending_files = []
    with open(f"{ray_dir}/{PENDING_FILES_PATH}", "r") as f:
        pending_files = f.readlines()
        pending_files = [file.strip() for file in pending_files]
    os.remove(f"{ray_dir}/{PENDING_FILES_PATH}")
    for i in range(len(pending_files)):
        if pending_files[i].split(".")[-1] != "py":
            pending_files[i] = pending_files[i].split(".")[0]
    return pending_files


def update_environment_pickle(ray_dir: str, pending_files: List[str]) -> None:
    """
    Update the environment pickle file with
    new source and doctree directory, and modify source file timestamps.
    """
    ray_doc_dir = os.path.join(ray_dir, "doc")
    with open(os.path.join(ray_doc_dir, ENVIRONMENT_PICKLE), "rb+") as f:
        env = pickle.load(f)
        # Update cache's environment source and doctree directory to the host path
        env.srcdir = os.path.join(ray_doc_dir, "source")
        env.doctreedir = os.path.join(ray_doc_dir, "_build/doctrees")
        env.project.srcdir = os.path.join(ray_doc_dir, "source")
        p = Project(
            os.path.join(ray_doc_dir, "source"),
            {".rst": "restructuredtext", ".md": "myst-nb", ".ipynb": "myst-nb"},
        )
        p.discover()
        env.project = p

        # all_docs is a map of source doc name -> last modified timestamp
        # Update timestamp of all docs, except the pending ones
        # to a later timestamp so they are not marked outdated and rebuilt.
        for doc, val in env.all_docs.items():
            if doc not in pending_files:
                env.all_docs[doc] = int(time.time()) * 1000000

    # Write the updated environment pickle file back
    with open(
        os.path.join(ray_doc_dir, "_build/doctrees/environment.pickle"), "wb+"
    ) as f:
        pickle.dump(env, f, pickle.HIGHEST_PROTOCOL)


# TODO(@khluu): Check if this is necessary. Only update changed template files.
def update_file_timestamp(ray_dir: str, pending_files: List[str]) -> None:
    """
    Update files other than source files to
    an old timestamp to avoid rebuilding them.
    """
    ray_doc_dir = os.path.join(ray_dir, "doc")

    # Update all target html files timestamp to the current time
    new_timestamp = datetime.now().timestamp()
    directory = f"{ray_doc_dir}/_build/html/"

    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                # Change the access and modification times
                os.utime(file_path, (new_timestamp, new_timestamp))
            except Exception as e:
                print(f"Failed to change timestamp for {file_path}: {str(e)}")

    # Update Makefile timestamp
    os.utime(f"{ray_doc_dir}/Makefile", (new_timestamp, new_timestamp))

    new_timestamp = datetime.now().timestamp()
    for file in pending_files:
        if file.split(".")[-1] != "py":
            continue
        file_path = os.path.join(ray_dir, file)
        try:
            # Change the access and modification times
            os.utime(file_path, (new_timestamp, new_timestamp))
        except Exception as e:
            print(f"Failed to change timestamp for {file_path}: {str(e)}")

    print("Timestamp change operation completed.")


@click.command()
@click.option("--ray-dir", required=True, type=str, help="Path to the Ray repository.")
def main(ray_dir: str) -> None:
    if not os.path.exists(f"{ray_dir}/{PENDING_FILES_PATH}"):
        print("Global cache was not loaded. Skip updating cache environment.")
        return
    print("Updating cache environment ...")
    pending_files = list_pending_files(ray_dir)
    update_environment_pickle(ray_dir, pending_files)
    update_file_timestamp(ray_dir, pending_files)


if __name__ == "__main__":
    main()
