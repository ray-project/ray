from ray.autoscaler._private.aws.config import key_pair
from ray.tests.aws.utils.constants import DEFAULT_KEY_PAIR


def mock_path_exists_key_pair(path):
    key_name, key_path = key_pair(0, "us-west-2", DEFAULT_KEY_PAIR["KeyName"])
    return path == key_path
