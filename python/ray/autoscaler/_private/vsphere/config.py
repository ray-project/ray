import copy
import logging
import os

from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from ray.autoscaler._private.constants import DISABLE_NODE_UPDATERS_KEY
from ray.autoscaler._private.event_system import CreateClusterEvent, global_event_system
from ray.autoscaler._private.util import check_legacy_fields

PRIVATE_KEY_NAME = "ray-bootstrap-key.pem"
PUBLIC_KEY_NAME = "ray_bootstrap_public_key.key"

PRIVATE_KEY_PATH = os.path.expanduser(f"~/{PRIVATE_KEY_NAME}")
PUBLIC_KEY_PATH = os.path.expanduser(f"~/{PUBLIC_KEY_NAME}")

logger = logging.getLogger(__name__)


def bootstrap_vsphere(config):

    # create a copy of the input config to modify
    config = copy.deepcopy(config)

    # Log warnings if user included deprecated `head_node` or `worker_nodes`
    # fields. Raise error if no `available_node_types`
    check_legacy_fields(config)

    # Configure SSH access, using an existing key pair if possible.
    config = configure_key_pair(config)
    # Configure docker run command to be executed on head and wroker nodes
    config = configure_run_options(config)

    global_event_system.execute_callback(
        CreateClusterEvent.ssh_keypair_downloaded,
        {"ssh_key_path": config["auth"]["ssh_private_key"]},
    )
    logger.info(f"{config}")
    return config


def configure_key_pair(config):
    logger.info("Configuring keys for Ray Cluster Launcher to ssh into the head node.")

    if not os.path.exists(PRIVATE_KEY_PATH):
        logger.warning(
            "Private key file at path {} was not found".format(PRIVATE_KEY_PATH)
        )
        _create_ssh_keys()
        logger.info(
            f"New SSH key pair {PRIVATE_KEY_PATH} and {PUBLIC_KEY_PATH} created."
        )

    # updater.py file uses the following config to ssh onto the head node
    # Also, copies the file onto the head node
    config["auth"]["ssh_private_key"] = PRIVATE_KEY_PATH
    # The path where the public key should be copied onto the remote host
    public_key_remote_path = f"~/{PUBLIC_KEY_NAME}"

    # Copy the public key to the remote host
    config["file_mounts"][public_key_remote_path] = PUBLIC_KEY_PATH

    return config


def configure_run_options(config):
    ssh_user = config["auth"]["ssh_user"]
    # By default enable TLS for Head-Worker grpc communication
    tls_enable = (
        1 if config["provider"]["vsphere_config"].get("tls_enable", True) else 0
    )
    # Configure common run options
    if "run_options" not in config["docker"]:
        config["docker"]["run_options"] = []
    config["docker"]["run_options"].append(f"--env RAY_USE_TLS={tls_enable}")

    # Configure head_run_options
    if "head_run_options" not in config["docker"]:
        config["docker"]["head_run_options"] = []
    config["docker"]["head_run_options"].append(
        f"--env-file /home/{ssh_user}/svc-account-token.env"
    )

    # Configure worker_run_options
    if "worker_run_options" not in config["docker"]:
        config["docker"]["worker_run_options"] = []

    if tls_enable == 1:
        # Generate TLS cert and key for head and worker nodes.
        # This needs to be done before ray start command
        config["head_start_ray_commands"].insert(0, "sh /home/ray/gencert.sh")
        config["worker_start_ray_commands"].insert(0, "sh /home/ray/gencert.sh")

        config["docker"]["run_options"].append(
            f"-v /home/{ssh_user}/ca.crt:/home/ray/ca.crt"
        )
        config["docker"]["run_options"].append(
            f"-v /home/{ssh_user}/ca.key:/home/ray/ca.key"
        )
        config["docker"]["run_options"].append(
            f"-v /home/{ssh_user}/gencert.sh:/home/ray/gencert.sh"
        )

        config["docker"]["run_options"].append("--env RAY_TLS_CA_CERT=/home/ray/ca.crt")
        config["docker"]["run_options"].append(
            "--env RAY_TLS_SERVER_KEY=/home/ray/tls.key"
        )
        config["docker"]["run_options"].append(
            "--env RAY_TLS_SERVER_CERT=/home/ray/tls.crt"
        )

    return config


def disable_node_updater(config):
    logger.info(
        "Disabling NodeUpdater threads as Cluster Operator is "
        + "responsible for Ray setup on nodes."
    )
    config["provider"][DISABLE_NODE_UPDATERS_KEY] = True
    return config


def _create_ssh_keys():
    """Create SSH keys as specified"""
    # Create a private key
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
    # Encode it in PEM format
    unencrypted_private_key = private_key.private_bytes(
        encoding=crypto_serialization.Encoding.PEM,
        format=crypto_serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=crypto_serialization.NoEncryption(),
    )
    # Create a public key
    public_key = private_key.public_key().public_bytes(
        encoding=crypto_serialization.Encoding.PEM,
        format=crypto_serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    # Write keys
    with open(PRIVATE_KEY_PATH, "wb") as pvt_key:
        # manage access mode for the pvt key
        os.chmod(PRIVATE_KEY_PATH, 0o600)
        pvt_key.write(unencrypted_private_key)

    with open(PUBLIC_KEY_PATH, "wb") as pub_key:
        # manage access mode for the pvt key
        pub_key.write(public_key)
