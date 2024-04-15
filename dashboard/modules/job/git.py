import subprocess
import os
import shutil
import tempfile
import asyncio
import pipes
import ray.dashboard.modules.job.id as id
import logging
from ray._private.runtime_env.utils import check_output_shell
from typing import Optional
from ray._private.runtime_env.utils import check_output_shell
from .history_server_storage import TOSStorage

default_logger = logging.getLogger(__name__)

# TOS endpoint and bucket, for example: tos://tos-cn-north.byted.org/inf-batch-ray/git
GIT_TOS_CACHE_URI = os.getenv("BYTED_RAY_GIT_TOS_CACHE_URI", None)
GIT_TOS_CACHE_KEY = os.getenv("BYTED_RAY_GIT_TOS_CACHE_KEY", None)


async def pull_submodules(
    target_dir,
    private_key,
    logger: Optional[logging.Logger] = default_logger,
    tmp_file=None,
    use_platform_git=False,
):
    if tmp_file is None:
        private_key = private_key
        fn = "{}.pem".format(id.generate_global_unique_id())
        tmp_file = "/opt/tiger/ray_pkgs/{}".format(fn)
        logger.info(f"tmp_file {tmp_file}")
        with open(tmp_file, "w") as f:
            f.write(private_key)
        os.chmod(tmp_file, 384)  # 600
    submodule_clone_cmd = "cd {} && GIT_SSH_COMMAND=\"ssh -i {} -o 'StrictHostKeyChecking=no' -F /dev/null\" git submodule update --init --recursive --depth=1".format(
        target_dir, tmp_file
    )
    if use_platform_git:
        submodule_clone_cmd = "cd {} && GIT_SSH_COMMAND=\"sshpass -f {} ssh -o 'StrictHostKeyChecking=no' -F /dev/null\" git submodule update --init --recursive --depth=1".format(
            target_dir, tmp_file
        )
    await check_output_shell(submodule_clone_cmd, logger=logger)


async def download_from_git(
    gitlab_repo_name,
    branch_name,
    commit_id,
    private_key,
    target_dir,
    use_platform_git: Optional[bool] = False,
    logger: Optional[logging.Logger] = default_logger,
    git_post_command: Optional[str] = None,
):
    home_path = os.getenv("HOME")
    gitignore_file = "{}/.gitconfig".format(home_path)
    if os.path.isfile(gitignore_file) and use_platform_git:
        os.remove(gitignore_file)

    if private_key is None:
        raise Exception("private_key is None from runtime env")

    cur_dir = os.getcwd()
    tos = None
    if GIT_TOS_CACHE_URI and GIT_TOS_CACHE_KEY:
        tos = GitTOSCache(gitlab_repo_name, commit_id, target_dir, logger)

    try:
        if not os.path.exists("/opt/tiger/ray_pkgs"):
            os.makedirs("/opt/tiger/ray_pkgs", exist_ok=True)

        # use job def template owner's rsa to download codes
        fn = "{}.pem".format(id.generate_global_unique_id())
        tmp_file = "/opt/tiger/ray_pkgs/{}".format(fn)
        logger.info(f"tmp_file {tmp_file}")
        with open(tmp_file, "w") as f:
            f.write(private_key)
        os.chmod(tmp_file, 384)  # 600

        # clear directory
        if os.path.exists(target_dir):
            logger.info(
                "[download_from_git] dir exist in mirror, try rm dir " + target_dir
            )
            shutil.rmtree(target_dir)
        # mkdir
        os.makedirs(target_dir)

        # 1. download git cache from tos
        if tos:
            success = await tos.download()
            if success:
                if git_post_command:
                    await run_post_command(
                        git_post_command, target_dir, tmp_file, logger
                    )
                return

        # 2. download git repo to folder by owner's id_rsa
        if commit_id:
            try:
                cmd = "cd {} && git init && git remote add origin git@code.byted.org:{}.git".format(
                    target_dir, pipes.quote(gitlab_repo_name)
                )
                if use_platform_git:
                    cmd = "cd {} && git init && git remote add origin gitlab@git.byted.org:{}.git".format(
                        target_dir, pipes.quote(gitlab_repo_name)
                    )
                await check_output_shell(cmd, logger=logger)
            except subprocess.CalledProcessError:
                raise Exception("git init error")

            cmd = "cd {} && GIT_SSH_COMMAND=\"ssh -i {} -o 'StrictHostKeyChecking=no' -F /dev/null\"  git fetch --depth 1 origin {}".format(
                target_dir, tmp_file, commit_id
            )
            if use_platform_git:
                cmd = "cd {} && GIT_SSH_COMMAND=\"sshpass -f {} ssh -o 'StrictHostKeyChecking=no' -F /dev/null\"  git fetch --depth 1 origin {}".format(
                    target_dir, tmp_file, commit_id
                )
            await check_output_shell(cmd, logger=logger)

            if branch_name:
                logger.info(f"get branch name, {branch_name}")
                try:
                    cmd = "cd {} && export GIT_SSH_COMMAND=\"ssh -i {} -o 'StrictHostKeyChecking=no' -F /dev/null\" && git checkout FETCH_HEAD && git checkout -b {}".format(
                        target_dir, tmp_file, pipes.quote(branch_name)
                    )
                    if use_platform_git:
                        cmd = "cd {} && export GIT_SSH_COMMAND=\"sshpass -f {} ssh -o 'StrictHostKeyChecking=no' -F /dev/null\" && git checkout FETCH_HEAD && git checkout -b {}".format(
                            target_dir, tmp_file, pipes.quote(branch_name)
                        )
                    await check_output_shell(
                        cmd,
                        logger=logger,
                    )
                except subprocess.CalledProcessError:
                    raise Exception("git checkout error")
            else:
                try:
                    cmd = "cd {} && export GIT_SSH_COMMAND=\"ssh -i {} -o 'StrictHostKeyChecking=no' -F /dev/null\" && git checkout {}".format(
                        target_dir, tmp_file, commit_id
                    )
                    if use_platform_git:
                        cmd = "cd {} && export GIT_SSH_COMMAND=\"sshpass -f {} ssh -o 'StrictHostKeyChecking=no' -F /dev/null\" && git checkout {}".format(
                            target_dir, tmp_file, commit_id
                        )
                    await check_output_shell(
                        cmd,
                        logger=logger,
                    )
                except subprocess.CalledProcessError:
                    raise Exception("git checkout error")

        # 3. download submudules with retry
        retrySuccess = False
        catchedError = ""

        home_path = os.getenv("HOME")
        gitignore_file = "{}/.gitconfig".format(home_path)

        if use_platform_git:
            if not os.path.isfile(gitignore_file):
                logger.info("modify git config")
                with open(gitignore_file, "a") as f:
                    f.write(
                        """
[url "gitlab@git.byted.org:"]
        insteadOf = https://code.byted.org/
[url "gitlab@git.byted.org"]
        insteadOf = git@code.byted.org
[url "gitlab@git.byted.org:"]
        insteadOf = ssh://git@code.byted.org/
[url "gerrit@git.byted.org:"]
        insteadOf = ssh://deploy@git.byted.org:29418/
[url "gerrit@git.byted.org:"]
        insteadOf = gitr:
[url "gerrit@git.byted.org:"]
        insteadOf = https://git.byted.org/
[url "gerrit@git.byted.org:"]
        insteadOf = https://review.byted.org/
                    """
                    )

        for _ in range(3):
            logger.info("====== retry {}======".format(pull_submodules.__name__))
            try:
                await pull_submodules(
                    target_dir, private_key, logger, tmp_file, use_platform_git
                )
            except Exception as e:
                catchedError = str(e)
            else:
                retrySuccess = True
                break

                await asyncio.sleep(2)

        if use_platform_git and os.path.isfile(gitignore_file):
            os.remove(gitignore_file)

        if not retrySuccess:
            raise RuntimeError(catchedError)

        # 4. run git_post_command with retry, is used for git lfs and so on
        if git_post_command:
            await run_post_command(git_post_command, target_dir, tmp_file, logger)

        # 5. upload to tos
        if tos:
            await tos.upload()
    except Exception as ex:
        if use_platform_git and os.path.isfile(gitignore_file):
            os.remove(gitignore_file)
        os.chdir(cur_dir)
        logger.info(f"get exception {ex}")
        raise RuntimeError(str(ex))

    os.chdir(cur_dir)


async def run_post_command(post_command, target_dir, tmp_file, logger):
    git_post_command = (
        "cd {} && export GIT_SSH_COMMAND=\"ssh -i {} -o 'StrictHostKeyChecking=no' -F /dev/null\" && ".format(
            target_dir, tmp_file
        )
        + post_command
    )
    logger.info("git post cmd: {}".format(git_post_command))

    retrySuccess = False
    catchedError = ""
    for _ in range(3):
        logger.info("====== retry git post command ======")
        try:
            await check_output_shell(git_post_command, logger=logger)
        except Exception as e:
            catchedError = str(e)
        else:
            retrySuccess = True
            break

        await asyncio.sleep(2)

    if not retrySuccess:
        raise RuntimeError(catchedError)


class GitTOSCache:
    def __init__(self, gitlab_repo_name, commit_id, target_dir, logger) -> None:
        uri = GIT_TOS_CACHE_URI
        key = GIT_TOS_CACHE_KEY
        assert uri and uri.startswith("tos://"), f"invaid uri: {uri}"

        self.tos = TOSStorage(uri, key, gitlab_repo_name, timeout=60)
        self.commit_id = commit_id
        self.target_dir = target_dir
        self.logger = logger

    async def upload(self):
        # tos cache already exists
        if self.tos.get_update_timestamp(self.commit_id):
            self.logger.info(f"{self.commit_id} tos cache already exists")
            return

        temp = os.path.join(tempfile.mkdtemp(), self.commit_id)
        # remove .git folder to reduce packing time
        cmd = "rm -rf {}/.git && tar -czvf {} -C {} .".format(
            self.target_dir, temp, self.target_dir
        )
        await check_output_shell(cmd, logger=self.logger)

        with open(temp, "rb") as f:
            data = f.read()
            try:
                self.tos.write(data, self.commit_id)
            except Exception as e:
                self.logger.error(f"{self.commit_id} tos cache write failed: {e}")
            else:
                self.logger.info(f"{self.commit_id} tos cache write succeed")

    async def download(self):
        # tos cache not exists
        if not self.tos.get_update_timestamp(self.commit_id):
            self.logger.info(f"{self.commit_id} tos cache not exists")
            return False

        # fetch git cache from tos
        data = self.tos.read(self.commit_id, decode=False)
        if data is None:
            self.logger.info(f"{self.commit_id} tos cache read failed")
            return False

        temp = os.path.join(tempfile.mkdtemp(), self.commit_id)
        with open(temp, "wb") as f:
            f.write(data)

        # extract tar file to target directory
        cmd = "tar -xzvf {} -C {}".format(temp, self.target_dir)
        await check_output_shell(cmd, logger=self.logger)

        self.logger.info(f"{self.commit_id} tos cache read succeed")
        return True
