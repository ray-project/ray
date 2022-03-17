import os
import shutil


def mock_s3_sync(local_path):
    def mocked(cmd, *args, **kwargs):
        binary = cmd[0]

        if binary != "aws":
            import subprocess

            return subprocess.check_call(cmd, *args, **kwargs)

        # The called command is e.g.
        # ["aws", "s3", "cp", "--recursive", "--quiet", local_path, bucket]
        source = cmd[5]
        target = cmd[6]

        checkpoint_path = os.path.join(local_path, "checkpoint")

        if source.startswith("s3://"):
            if os.path.exists(target):
                shutil.rmtree(target)
            shutil.copytree(checkpoint_path, target)
        else:
            if os.path.exists(checkpoint_path):
                shutil.rmtree(checkpoint_path)
            shutil.copytree(source, checkpoint_path)

    return mocked
