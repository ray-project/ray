from ci.ray_ci.automation.docker_tags_lib import delete_old_commit_tags
import os

def main():
    bazel_workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")
    #print(bazel_workspace_dir)
    delete_old_commit_tags(
        namespace="rayproject",
        repository="ray",
        n_days=30,
        num_tags=1000,
    )

if __name__ == "__main__":
    main()