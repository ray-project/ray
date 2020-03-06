#!/usr/bin/env python

import sys
import oss2
import os
import logging

OSS_URL = "http://oss-cn-beijing.aliyuncs.com"
OSS_BUCKET_NAME = "ray-jars"


logger = logging.getLogger(__name__)


def get_oss_access_key():
    return os.environ["OSS_ACCESS_KEY"]


def get_oss_access_key_secret():
    return os.environ["OSS_ACCESS_KEY_SECRET"]


# A helper method to locate the current dir.
def get_current_dir():
    this_dir = os.path.dirname(os.path.realpath(__file__))
    return this_dir


# Get all files from the given directory.
def get_all_files(dir):
    all_files = []
    list = os.listdir(dir)
    for i in range(0, len(list)):
        path = os.path.join(dir, list[i])
        if os.path.isfile(path):
            all_files.append(list[i])
    return all_files


def get_platform_sign():
    if sys.platform == "darwin":
        return "os_mac_osx"
    elif sys.platform == "linux":
        return "os_linux"


def upload_native_files(commit_id):
    auth = oss2.Auth(get_oss_access_key(), get_oss_access_key_secret())
    bucket = oss2.Bucket(auth, OSS_URL, OSS_BUCKET_NAME)
    native_files_dir = os.path.join(get_current_dir(), "../runtime/native_dependencies")
    for native_filename in get_all_files(native_files_dir):
        dest_filename = os.path.join(commit_id, get_platform_sign(), native_filename)
        bucket.put_object_from_file(dest_filename, os.path.join(native_files_dir, native_filename))
        logger.warning("Succeeded to upload [" + native_filename + "] to [" + dest_filename + "]")


def get_all_files_from_oss(bucket, commit_id):
    result = []
    for obj in oss2.ObjectIterator(bucket, prefix="{}/".format(commit_id)):
        if not obj.is_prefix():
            # `obj` is a not a directory.
            if not obj.key.endswith("/"):
                result.append(obj.key)
    return result

# Prepare the directory of native native_dependencies to avoid no such file when downloading.
def _prepare_native_dependencies_dir():
    dir = os.path.join(get_current_dir(), "../runtime/native_dependencies")
    if not os.path.exists(dir):
        os.makedirs(dir)

def download_all_native_files(commit_id):
    auth = oss2.Auth(get_oss_access_key(), get_oss_access_key_secret())
    bucket = oss2.Bucket(auth, OSS_URL, OSS_BUCKET_NAME)
    native_files = get_all_files_from_oss(bucket, commit_id)
    _prepare_native_dependencies_dir()
    for object_file in native_files:
        dest_filename = os.path.join(get_current_dir(), "../runtime/native_dependencies", os.path.split(object_file)[1])
        if dest_filename.endswith(".jar"):
            # Skip downloading jars.
            continue
        bucket.get_object_to_file(object_file, dest_filename)
        logger.warning("Succeeded to download [" + object_file + "] to [" + dest_filename +"]")

def upload_all_in_one_jar(commit_id):
    auth = oss2.Auth(get_oss_access_key(), get_oss_access_key_secret())
    bucket = oss2.Bucket(auth, OSS_URL, OSS_BUCKET_NAME)
    # TODO(qwang): Do not use fixed version here.
    ray_api_jar_filename = "ray-api-0.1-SNAPSHOT.jar"
    ray_runtime_jar_filename = "ray-runtime-0.1-SNAPSHOT.jar"
    ray_api_jar_file = os.path.join(get_current_dir(), "../tutorial/lib", ray_api_jar_filename)
    ray_runtime_jar_file = os.path.join(get_current_dir(), "../tutorial/lib", ray_runtime_jar_filename)
    bucket.put_object_from_file(os.path.join(commit_id, ray_api_jar_filename), ray_api_jar_file)
    bucket.put_object_from_file(os.path.join(commit_id, ray_runtime_jar_filename), ray_runtime_jar_file)
    logger.info("Succeeded to update ray jars to oss.")

def main(commit_id, command_name):
    if command_name == "upload":
        upload_native_files(commit_id)
    elif command_name == "download":
        download_all_native_files(commit_id)
    elif command_name == "upload-jars":
        upload_all_in_one_jar(commit_id)
    else:
        raise ValueError("Unknown command.")


if __name__ == "__main__":
    commit_id = sys.argv[1]
    command_name = sys.argv[2]
    main(commit_id, command_name)
