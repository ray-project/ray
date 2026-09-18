"""Bootstrap logic for the OCI node provider.

``bootstrap_oci`` fills in everything a minimal cluster config leaves out so
that ``ray up`` works with just a compartment and a region:

* availability domain (first AD of the region unless configured),
* networking (an idempotent ``ray-autoscaler-vcn`` with a public subnet,
  internet gateway, route rule and security list) unless ``subnet_id`` is set,
* IAM for the head node (a dynamic group matching instances in the
  compartment plus a policy granting it compute/network rights) so the
  autoscaler on the head can authenticate with instance principals,
* an SSH key pair injected through instance metadata ``ssh_authorized_keys``,
* the newest Canonical Ubuntu platform image compatible with each shape.

Every step is idempotent: re-running ``ray up`` reuses resources created by a
previous run, identified by display name and a ``ray-autoscaler`` free-form
tag.
"""

import copy
import hashlib
import logging
import os
import time
from functools import partial
from typing import Any, Dict, List, Optional

from ray.autoscaler._private._oci.utils import (
    OCIClient,
    dns_label,
    is_not_found_or_not_authorized,
    short_id,
)
from ray.autoscaler._private.cli_logger import cf, cli_logger
from ray.autoscaler._private.util import check_legacy_fields, generate_rsa_key_pair

logger = logging.getLogger(__name__)

RAY = "ray-autoscaler"
# Tag placed on every resource the launcher creates so that re-runs (and
# humans) can recognise them.
MANAGED_BY_TAG = {"ray-autoscaler": "true"}

DEFAULT_VCN_NAME = f"{RAY}-vcn"
DEFAULT_VCN_CIDR = "10.77.0.0/16"
DEFAULT_SUBNET_NAME = f"{RAY}-subnet"
DEFAULT_SUBNET_CIDR = "10.77.0.0/24"
DEFAULT_IGW_NAME = f"{RAY}-igw"
INTERNET_CIDR = "0.0.0.0/0"

DEFAULT_IMAGE_OS = "Canonical Ubuntu"
DEFAULT_IMAGE_OS_VERSION = "22.04"

# IAM statements granted to the head node's dynamic group. Mirrors the
# EC2FullAccess-style role the AWS provider creates for its head node.
IAM_POLICY_STATEMENTS = [
    "Allow dynamic-group id {dg} to manage instance-family in compartment id {c}",
    "Allow dynamic-group id {dg} to use virtual-network-family in compartment id {c}",
    "Allow dynamic-group id {dg} to manage volume-family in compartment id {c}",
    "Allow dynamic-group id {dg} to read app-catalog-listing in compartment id {c}",
]

# Seconds to wait for freshly created network/IAM resources to become usable.
RESOURCE_WAIT_S = 120
IAM_PROPAGATION_WAIT_S = 20


def bootstrap_oci(config: Dict[str, Any]) -> Dict[str, Any]:
    config = copy.deepcopy(config)
    check_legacy_fields(config)
    _validate_provider(config["provider"])

    client = OCIClient(config["provider"])
    cli_logger.verbose(
        "Authenticated with OCI using {} for compartment ...{}",
        cf.bold(client.auth_mode),
        short_id(config["provider"]["compartment_id"]),
    )

    _configure_availability_domain(config, client)
    _configure_network(config, client)
    _configure_iam(config, client)
    _configure_key_pair(config)
    _configure_images(config, client)
    return config


def _validate_provider(provider: Dict[str, Any]) -> None:
    for key in ("region", "compartment_id"):
        if not provider.get(key):
            cli_logger.abort(
                f"The OCI node provider requires `provider.{key}` to be set in the "
                "cluster config."
            )
    if not str(provider["compartment_id"]).startswith(
        ("ocid1.compartment.", "ocid1.tenancy.")
    ):
        cli_logger.abort(
            "`provider.compartment_id` must be a compartment or tenancy OCID, "
            f"got {provider['compartment_id']}."
        )


def _node_types(config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return config["available_node_types"]


# ----------------------------------------------------------------------
# Availability domain
# ----------------------------------------------------------------------
def _configure_availability_domain(config: Dict[str, Any], client: OCIClient):
    provider = config["provider"]
    if provider.get("availability_domain"):
        return
    ads = client.list_all(
        client.identity().list_availability_domains, provider["compartment_id"]
    )
    if not ads:
        cli_logger.abort(
            "No availability domains are visible in compartment "
            f"...{short_id(provider['compartment_id'])}."
        )
    provider["availability_domain"] = ads[0].name
    cli_logger.verbose(
        "Using availability domain {}", cf.bold(provider["availability_domain"])
    )


# ----------------------------------------------------------------------
# Networking
# ----------------------------------------------------------------------
def _find_by_name(items: List[Any], name: str) -> Optional[Any]:
    for item in items:
        if item.display_name == name and item.lifecycle_state not in (
            "TERMINATING",
            "TERMINATED",
        ):
            return item
    return None


def _wait_available(client: OCIClient, get_fn, resource_id: str, what: str):
    deadline = time.time() + RESOURCE_WAIT_S
    while True:
        resource = get_fn(resource_id).data
        if resource.lifecycle_state == "AVAILABLE":
            return resource
        if time.time() > deadline:
            cli_logger.abort(
                f"{what} ...{short_id(resource_id)} did not become AVAILABLE within "
                f"{RESOURCE_WAIT_S} seconds (state {resource.lifecycle_state})."
            )
        time.sleep(3)


def _configure_network(config: Dict[str, Any], client: OCIClient) -> None:
    """Ensure every node type has a subnet, creating the default VCN if needed."""
    provider = config["provider"]
    node_types = _node_types(config)

    needs_default = any(
        "subnet_id" not in nt.get("node_config", {}) for nt in node_types.values()
    ) and not provider.get("subnet_id")
    if needs_default:
        provider["subnet_id"] = _get_or_create_vcn_subnet(config, client)

    for name, node_type in node_types.items():
        node_config = node_type.setdefault("node_config", {})
        node_config.setdefault("subnet_id", provider.get("subnet_id"))
        cli_logger.verbose(
            "Node type {} uses subnet ...{}",
            cf.bold(name),
            short_id(node_config["subnet_id"]),
        )


def _get_or_create_vcn_subnet(config: Dict[str, Any], client: OCIClient) -> str:
    provider = config["provider"]
    models = client.models()
    network = client.network
    compartment_id = provider["compartment_id"]
    vcn_name = provider.get("vcn_name", DEFAULT_VCN_NAME)
    vcn_cidr = provider.get("vcn_cidr", DEFAULT_VCN_CIDR)
    subnet_name = DEFAULT_SUBNET_NAME
    subnet_cidr = provider.get("subnet_cidr", DEFAULT_SUBNET_CIDR)

    vcn = _find_by_name(
        client.list_all(network.list_vcns, compartment_id, display_name=vcn_name),
        vcn_name,
    )
    if vcn is None:
        cli_logger.print(
            "Creating VCN {} ({}) in compartment ...{}",
            cf.bold(vcn_name),
            vcn_cidr,
            short_id(compartment_id),
        )
        vcn = network.create_vcn(
            models.CreateVcnDetails(
                compartment_id=compartment_id,
                cidr_blocks=[vcn_cidr],
                display_name=vcn_name,
                dns_label=dns_label(vcn_name),
                freeform_tags=MANAGED_BY_TAG,
            )
        ).data
        vcn = _wait_available(client, network.get_vcn, vcn.id, "VCN")
    else:
        cli_logger.verbose("Reusing VCN {} ...{}", cf.bold(vcn_name), short_id(vcn.id))

    # Internet gateway.
    igw = _find_by_name(
        client.list_all(
            network.list_internet_gateways,
            compartment_id,
            vcn_id=vcn.id,
            display_name=DEFAULT_IGW_NAME,
        ),
        DEFAULT_IGW_NAME,
    )
    if igw is None:
        cli_logger.verbose("Creating internet gateway {}", cf.bold(DEFAULT_IGW_NAME))
        igw = network.create_internet_gateway(
            models.CreateInternetGatewayDetails(
                compartment_id=compartment_id,
                vcn_id=vcn.id,
                is_enabled=True,
                display_name=DEFAULT_IGW_NAME,
                freeform_tags=MANAGED_BY_TAG,
            )
        ).data
        igw = _wait_available(
            client, network.get_internet_gateway, igw.id, "Internet gateway"
        )

    # Default route table: 0.0.0.0/0 -> internet gateway.
    route_table = network.get_route_table(vcn.default_route_table_id).data
    if not any(
        rule.network_entity_id == igw.id and rule.destination == INTERNET_CIDR
        for rule in route_table.route_rules
    ):
        cli_logger.verbose("Adding default route to the internet gateway")
        network.update_route_table(
            vcn.default_route_table_id,
            models.UpdateRouteTableDetails(
                route_rules=list(route_table.route_rules)
                + [
                    models.RouteRule(
                        destination=INTERNET_CIDR,
                        destination_type="CIDR_BLOCK",
                        network_entity_id=igw.id,
                        description="Ray autoscaler: internet access",
                    )
                ]
            ),
        )

    # Default security list: SSH from anywhere, everything inside the VCN,
    # all egress.
    security_list = network.get_security_list(vcn.default_security_list_id).data
    desired_ingress = [
        models.IngressSecurityRule(
            protocol="6",
            source=INTERNET_CIDR,
            source_type="CIDR_BLOCK",
            is_stateless=False,
            tcp_options=models.TcpOptions(
                destination_port_range=models.PortRange(min=22, max=22)
            ),
            description="Ray autoscaler: SSH",
        ),
        models.IngressSecurityRule(
            protocol="all",
            source=vcn_cidr,
            source_type="CIDR_BLOCK",
            is_stateless=False,
            description="Ray autoscaler: intra-cluster traffic",
        ),
    ]
    desired_egress = [
        models.EgressSecurityRule(
            protocol="all",
            destination=INTERNET_CIDR,
            destination_type="CIDR_BLOCK",
            is_stateless=False,
            description="Ray autoscaler: all egress",
        )
    ]
    existing_descriptions = {
        rule.description
        for rule in list(security_list.ingress_security_rules)
        + list(security_list.egress_security_rules)
    }
    if not all(
        rule.description in existing_descriptions
        for rule in desired_ingress + desired_egress
    ):
        cli_logger.verbose("Updating the VCN default security list")
        network.update_security_list(
            vcn.default_security_list_id,
            models.UpdateSecurityListDetails(
                ingress_security_rules=[
                    r
                    for r in security_list.ingress_security_rules
                    if r.description not in {d.description for d in desired_ingress}
                ]
                + desired_ingress,
                egress_security_rules=[
                    r
                    for r in security_list.egress_security_rules
                    if r.description not in {d.description for d in desired_egress}
                ]
                + desired_egress,
            ),
        )

    # Public, regional subnet.
    subnet = _find_by_name(
        client.list_all(
            network.list_subnets,
            compartment_id,
            vcn_id=vcn.id,
            display_name=subnet_name,
        ),
        subnet_name,
    )
    if subnet is None:
        cli_logger.print("Creating subnet {} ({})", cf.bold(subnet_name), subnet_cidr)
        subnet = network.create_subnet(
            models.CreateSubnetDetails(
                compartment_id=compartment_id,
                vcn_id=vcn.id,
                cidr_block=subnet_cidr,
                display_name=subnet_name,
                dns_label=dns_label(subnet_name),
                prohibit_public_ip_on_vnic=False,
                prohibit_internet_ingress=False,
                route_table_id=vcn.default_route_table_id,
                security_list_ids=[vcn.default_security_list_id],
                freeform_tags=MANAGED_BY_TAG,
            )
        ).data
        subnet = _wait_available(client, network.get_subnet, subnet.id, "Subnet")
    else:
        cli_logger.verbose(
            "Reusing subnet {} ...{}", cf.bold(subnet_name), short_id(subnet.id)
        )
    return subnet.id


# ----------------------------------------------------------------------
# IAM (instance principals for the head node)
# ----------------------------------------------------------------------
def _iam_resource_name(compartment_id: str) -> str:
    digest = hashlib.sha256(compartment_id.encode()).hexdigest()[:8]
    return f"{RAY}-{digest}"


def _configure_iam(config: Dict[str, Any], client: OCIClient) -> None:
    """Create the dynamic group + policy that let the head node's autoscaler
    launch and terminate instances via instance principals."""
    provider = config["provider"]
    if not provider.get("use_instance_principal", True):
        cli_logger.verbose(
            "`use_instance_principal` is false; the head node must receive OCI "
            "credentials through `file_mounts`."
        )
        return
    if not provider.get("create_iam_resources", True):
        cli_logger.verbose(
            "`create_iam_resources` is false; assuming a dynamic group and "
            "policy already grant instance principals in the compartment the "
            "required permissions."
        )
        return

    compartment_id = provider["compartment_id"]
    name = provider.get("dynamic_group_name") or _iam_resource_name(compartment_id)
    tenancy_id = client.tenancy_of_compartment(compartment_id)
    home_region = client.home_region_of_tenancy(tenancy_id)
    identity = client.identity(home_region)
    models = client.identity_models()

    def _denied(what: str, exc: Exception):
        statements = "\n    ".join(
            s.format(dg="<dynamic group OCID>", c="<compartment OCID>")
            for s in IAM_POLICY_STATEMENTS
        )
        cli_logger.abort(
            f"Could not create the {what} {name} for instance principal "
            f"authentication ({exc}).\n"
            "Ask a tenancy administrator to create a dynamic group whose matching "
            "rule selects instances in the compartment "
            "(instance.compartment.id = '<compartment OCID>') and a policy "
            "granting it the following statements, then set "
            f"`create_iam_resources: false` under `provider`:\n    {statements}\n"
            "Alternatively set `use_instance_principal: false` and copy OCI "
            "credentials to the head node with `file_mounts`."
        )

    matching_rule = f"ALL {{instance.compartment.id = '{compartment_id}'}}"
    groups = [
        g
        for g in client.list_all(identity.list_dynamic_groups, tenancy_id, name=name)
        if g.lifecycle_state == "ACTIVE"
    ]
    if groups:
        dynamic_group = groups[0]
        cli_logger.verbose("Reusing dynamic group {}", cf.bold(name))
    else:
        cli_logger.print("Creating dynamic group {} for the head node", cf.bold(name))
        try:
            dynamic_group = identity.create_dynamic_group(
                models.CreateDynamicGroupDetails(
                    compartment_id=tenancy_id,
                    name=name,
                    matching_rule=matching_rule,
                    description=(
                        "Instances launched by the Ray autoscaler in compartment "
                        f"{compartment_id}"
                    ),
                    freeform_tags=MANAGED_BY_TAG,
                )
            ).data
        except Exception as e:  # noqa: BLE001
            if is_not_found_or_not_authorized(e):
                _denied("dynamic group", e)
            raise

    statements = [
        s.format(dg=dynamic_group.id, c=compartment_id) for s in IAM_POLICY_STATEMENTS
    ]
    policies = [
        p
        for p in client.list_all(identity.list_policies, compartment_id, name=name)
        if p.lifecycle_state == "ACTIVE"
    ]
    if policies:
        policy = policies[0]
        if set(statements) - set(policy.statements):
            cli_logger.verbose("Updating policy {}", cf.bold(name))
            identity.update_policy(
                policy.id,
                models.UpdatePolicyDetails(
                    statements=sorted(set(policy.statements) | set(statements))
                ),
            )
        else:
            cli_logger.verbose("Reusing policy {}", cf.bold(name))
    else:
        cli_logger.print("Creating IAM policy {} for the head node", cf.bold(name))
        try:
            identity.create_policy(
                models.CreatePolicyDetails(
                    compartment_id=compartment_id,
                    name=name,
                    statements=statements,
                    description="Permissions for the Ray autoscaler head node",
                    freeform_tags=MANAGED_BY_TAG,
                )
            )
        except Exception as e:  # noqa: BLE001
            if is_not_found_or_not_authorized(e):
                _denied("policy", e)
            raise
        # IAM changes take a little while to propagate to the compute plane.
        time.sleep(IAM_PROPAGATION_WAIT_S)

    provider["dynamic_group_name"] = name


# ----------------------------------------------------------------------
# SSH key pair
# ----------------------------------------------------------------------
def _key_paths(region: str):
    private_key_path = os.path.expanduser(f"~/.ssh/{RAY}_oci_{region}.pem")
    return private_key_path, private_key_path[: -len(".pem")] + ".pub"


def _public_key_from_private(private_key_path: str) -> str:
    from cryptography.hazmat.primitives import serialization

    with open(private_key_path, "rb") as f:
        key = serialization.load_pem_private_key(f.read(), password=None)
    return (
        key.public_key()
        .public_bytes(
            serialization.Encoding.OpenSSH, serialization.PublicFormat.OpenSSH
        )
        .decode("utf-8")
    )


def _configure_key_pair(config: Dict[str, Any]) -> None:
    """Pick (or generate) an SSH key and inject the public half into each
    node type's instance metadata, the OCI equivalent of an EC2 key pair.

    Only ``auth.ssh_private_key`` is kept in the resulting config; the
    launcher copies that file to the head node as ``~/ray_bootstrap_key.pem``.
    """
    auth = config.setdefault("auth", {})
    auth.setdefault("ssh_user", "ubuntu")

    if "ssh_private_key" in auth:
        private_key_path = os.path.expanduser(auth["ssh_private_key"])
        if "ssh_public_key" in auth:
            with open(os.path.expanduser(auth["ssh_public_key"])) as f:
                public_key = f.read().strip()
        elif os.path.exists(private_key_path + ".pub"):
            with open(private_key_path + ".pub") as f:
                public_key = f.read().strip()
        else:
            public_key = _public_key_from_private(private_key_path)
    else:
        private_key_path, public_key_path = _key_paths(config["provider"]["region"])
        if os.path.exists(private_key_path):
            cli_logger.verbose("Reusing SSH key {}", cf.bold(private_key_path))
            if os.path.exists(public_key_path):
                with open(public_key_path) as f:
                    public_key = f.read().strip()
            else:
                public_key = _public_key_from_private(private_key_path)
        else:
            cli_logger.print("Creating SSH key pair {}", cf.bold(private_key_path))
            os.makedirs(os.path.dirname(private_key_path), exist_ok=True)
            public_key, pem = generate_rsa_key_pair()
            with open(private_key_path, "w", opener=partial(os.open, mode=0o600)) as f:
                f.write(pem)
            with open(public_key_path, "w") as f:
                f.write(public_key + "\n")
        auth["ssh_private_key"] = private_key_path

    # The public key only needs to reach the instances (below). Keep it out of
    # `auth`: the head node hashes launch configs with the key files listed
    # there, and a workstation path would not exist on the head.
    auth.pop("ssh_public_key", None)

    for node_type in _node_types(config).values():
        metadata = node_type.setdefault("node_config", {}).setdefault("metadata", {})
        existing = metadata.get("ssh_authorized_keys", "")
        if public_key not in existing:
            metadata["ssh_authorized_keys"] = (
                existing.rstrip("\n") + "\n" + public_key
            ).strip("\n")


# ----------------------------------------------------------------------
# Images
# ----------------------------------------------------------------------
def _resolve_image(
    client: OCIClient,
    compartment_id: str,
    shape: str,
    operating_system: str,
    operating_system_version: str,
    cache: Dict[str, str],
) -> str:
    cache_key = f"{shape}/{operating_system}/{operating_system_version}"
    if cache_key in cache:
        return cache[cache_key]
    images = client.list_all(
        client.compute.list_images,
        compartment_id,
        operating_system=operating_system,
        operating_system_version=operating_system_version,
        shape=shape,
        sort_by="TIMECREATED",
        sort_order="DESC",
        lifecycle_state="AVAILABLE",
    )
    # Prefer full (non-"Minimal") images, newest first.
    full_images = [im for im in images if "Minimal" not in (im.display_name or "")]
    images = sorted(full_images or images, key=lambda im: im.time_created, reverse=True)
    if not images:
        cli_logger.abort(
            f"No {operating_system} {operating_system_version} platform image is "
            f"available for shape {shape} in region {client.region}. "
            "Set `image_id` in `node_config` explicitly."
        )
    image = images[0]
    cli_logger.verbose(
        "Resolved image {} (...{}) for shape {}",
        cf.bold(image.display_name),
        short_id(image.id),
        shape,
    )
    cache[cache_key] = image.id
    return image.id


def _configure_images(config: Dict[str, Any], client: OCIClient) -> None:
    provider = config["provider"]
    cache: Dict[str, str] = {}
    for name, node_type in _node_types(config).items():
        node_config = node_type.setdefault("node_config", {})
        source = node_config.get("source_details") or {}
        if node_config.get("image_id") or source.get("image_id"):
            continue
        if "shape" not in node_config:
            cli_logger.abort(f"Node type {name} is missing `node_config.shape`.")
        node_config["image_id"] = _resolve_image(
            client,
            provider["compartment_id"],
            node_config["shape"],
            node_config.get("image_operating_system", DEFAULT_IMAGE_OS),
            node_config.get("image_operating_system_version", DEFAULT_IMAGE_OS_VERSION),
            cache,
        )


# ----------------------------------------------------------------------
# Resource autodetection
# ----------------------------------------------------------------------
def fillout_resources(config: Dict[str, Any]) -> Dict[str, Any]:
    """Fill in ``resources`` (CPU/GPU) for node types from their shape.

    Flexible shapes take the OCPU count from ``shape_config``; on x86 one OCPU
    is two vCPUs (hyperthreads), on Ampere A1 one OCPU is one vCPU. GPU counts
    come from the shape description. User-provided values always win.
    """
    if "available_node_types" not in config:
        return config
    config = copy.deepcopy(config)
    provider = config["provider"]
    client = OCIClient(provider)
    kwargs = {}
    if provider.get("availability_domain"):
        kwargs["availability_domain"] = provider["availability_domain"]
    shapes = {
        s.shape: s
        for s in client.list_all(
            client.compute.list_shapes, provider["compartment_id"], **kwargs
        )
    }
    for node_type in config["available_node_types"].values():
        node_config = node_type.get("node_config", {})
        shape = shapes.get(node_config.get("shape"))
        if shape is None:
            continue
        detected: Dict[str, int] = {}
        ocpus = (node_config.get("shape_config") or {}).get("ocpus") or shape.ocpus
        if ocpus:
            threads_per_ocpu = (
                1 if "Ampere" in (shape.processor_description or "") else 2
            )
            detected["CPU"] = int(round(float(ocpus) * threads_per_ocpu))
        if shape.gpus:
            detected["GPU"] = int(shape.gpus)
        detected.update(node_type.get("resources") or {})
        node_type["resources"] = detected
    return config
