"""Unit tests for the OCI node provider.

The OCI SDK is not a Ray test dependency, so these tests install a small
in-memory fake of the ``oci`` package into ``sys.modules``. The fake models
the subset of the Compute, Virtual Network and Identity APIs the provider
uses, including lifecycle states, free-form tags and pagination.
"""

import copy
import os
import sys
import threading
import types
from typing import Any, Dict, List

import click
import pytest

from ray.autoscaler.node_launch_exception import NodeLaunchException
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    NODE_KIND_WORKER,
    TAG_RAY_CLUSTER_NAME,
    TAG_RAY_LAUNCH_CONFIG,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_NAME,
    TAG_RAY_NODE_STATUS,
    TAG_RAY_USER_NODE_TYPE,
)

COMPARTMENT = "ocid1.compartment.oc1..testcompartment"
TENANCY = "ocid1.tenancy.oc1..testtenancy"
REGION = "us-phoenix-1"
AD = "Uocm:PHX-AD-1"
AD2 = "Uocm:PHX-AD-2"

# ---------------------------------------------------------------------------
# Fake OCI SDK
# ---------------------------------------------------------------------------


class FakeServiceError(Exception):
    def __init__(self, status, code, headers=None, message=""):
        super().__init__(message)
        self.status = status
        self.code = code
        self.headers = headers or {}
        self.message = message


# The provider recognises SDK errors by class name.
FakeServiceError.__name__ = "ServiceError"


class FakeProfileNotFound(Exception):
    pass


class FakeModel:
    """Stand-in for any generated SDK model: attributes default to None."""

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)
        return None

    def __repr__(self):
        return f"{type(self).__name__}({self.__dict__})"


class _Models:
    """Namespace that hands out a FakeModel subclass for any model name."""

    def __init__(self):
        self._classes = {}

    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)
        if name not in self._classes:
            self._classes[name] = type(name, (FakeModel,), {})
        return self._classes[name]


class Response:
    def __init__(self, data):
        self.data = data


class FakeState:
    """Shared backing store for the fake clients."""

    def __init__(self):
        self.instances: Dict[str, FakeModel] = {}
        self.vnics: Dict[str, FakeModel] = {}
        self.vnic_attachments: Dict[str, List[FakeModel]] = {}
        self.vcns: Dict[str, FakeModel] = {}
        self.internet_gateways: Dict[str, FakeModel] = {}
        self.route_tables: Dict[str, FakeModel] = {}
        self.security_lists: Dict[str, FakeModel] = {}
        self.subnets: Dict[str, FakeModel] = {}
        self.images: List[FakeModel] = []
        self.shapes: List[FakeModel] = []
        self.dynamic_groups: Dict[str, FakeModel] = {}
        self.policies: Dict[str, FakeModel] = {}
        self.calls: List[str] = []
        self.conflicts: Dict[str, int] = {}
        self.launch_error = None
        self.deny_iam = False
        self.deny_get_compartment = False
        self.counter = 0

    def new_id(self, kind):
        self.counter += 1
        return f"ocid1.{kind}.oc1.phx.fake{self.counter:04d}"

    def add_instance(
        self, tags, state="RUNNING", name="node", shape="VM.Standard.E4.Flex", ad=AD
    ):
        instance_id = self.new_id("instance")
        self.instances[instance_id] = FakeModel(
            id=instance_id,
            display_name=name,
            lifecycle_state=state,
            freeform_tags=dict(tags),
            shape=shape,
            availability_domain=ad,
            compartment_id=COMPARTMENT,
        )
        return instance_id

    def attach_vnic(self, instance_id, private_ip, public_ip):
        vnic_id = self.new_id("vnic")
        self.vnics[vnic_id] = FakeModel(
            id=vnic_id, private_ip=private_ip, public_ip=public_ip, is_primary=True
        )
        self.vnic_attachments.setdefault(instance_id, []).append(
            FakeModel(
                lifecycle_state="ATTACHED", vnic_id=vnic_id, instance_id=instance_id
            )
        )


class FakeComputeClient:
    def __init__(self, config, **kwargs):
        self.region = config.get("region")
        self.state = STATE

    def _maybe_conflict(self, name):
        if self.state.conflicts.get(name, 0) > 0:
            self.state.conflicts[name] -= 1
            self.state.calls.append(f"{name}:409")
            raise FakeServiceError(
                409, "Conflict", message="instance is currently being modified"
            )

    def list_instances(self, compartment_id, **kwargs):
        ad = kwargs.get("availability_domain")
        self.state.calls.append(f"list_instances:{ad}")
        assert compartment_id == COMPARTMENT
        # Like the real API, an availability_domain filter hides other ADs.
        return Response(
            [
                inst
                for inst in self.state.instances.values()
                if ad is None or inst.availability_domain == ad
            ]
        )

    def get_instance(self, instance_id, **kwargs):
        instance = self.state.instances.get(instance_id)
        if instance is None:
            raise FakeServiceError(404, "NotAuthorizedOrNotFound", message="missing")
        return Response(instance)

    def launch_instance(self, details, **kwargs):
        self.state.calls.append("launch_instance")
        if self.state.launch_error is not None:
            raise self.state.launch_error
        instance_id = self.state.new_id("instance")
        instance = FakeModel(
            id=instance_id,
            display_name=details.display_name,
            lifecycle_state="PROVISIONING",
            freeform_tags=dict(details.freeform_tags),
            shape=details.shape,
            availability_domain=details.availability_domain,
            compartment_id=details.compartment_id,
            launch_details=details,
        )
        self.state.instances[instance_id] = instance
        return Response(instance)

    def update_instance(self, instance_id, details, **kwargs):
        self._maybe_conflict("update_instance")
        self.state.calls.append("update_instance")
        instance = self.get_instance(instance_id).data
        instance.freeform_tags = dict(details.freeform_tags)
        return Response(instance)

    def terminate_instance(self, instance_id, **kwargs):
        self._maybe_conflict("terminate_instance")
        self.state.calls.append(
            f"terminate_instance:{kwargs.get('preserve_boot_volume')}"
        )
        instance = self.get_instance(instance_id).data
        instance.lifecycle_state = "TERMINATED"
        return Response(None)

    def instance_action(self, instance_id, action, **kwargs):
        self.state.calls.append(f"instance_action:{action}")
        instance = self.get_instance(instance_id).data
        instance.lifecycle_state = {"STOP": "STOPPED", "START": "RUNNING"}[action]
        return Response(instance)

    def list_vnic_attachments(self, compartment_id, **kwargs):
        return Response(
            list(self.state.vnic_attachments.get(kwargs["instance_id"], []))
        )

    def list_images(self, compartment_id, **kwargs):
        self.state.calls.append("list_images")
        images = [
            im
            for im in self.state.images
            if im.operating_system == kwargs.get("operating_system")
            and im.operating_system_version == kwargs.get("operating_system_version")
            and kwargs.get("shape") in im.shapes
        ]
        return Response(images)

    def list_shapes(self, compartment_id, **kwargs):
        ad = kwargs.get("availability_domain")
        self.state.calls.append(f"list_shapes:{ad}")
        # Shapes may be restricted to some ADs (``availability_domains``).
        return Response(
            [
                shape
                for shape in self.state.shapes
                if ad is None
                or not shape.availability_domains
                or ad in shape.availability_domains
            ]
        )


class FakeNetworkClient:
    def __init__(self, config, **kwargs):
        self.state = STATE

    def _create(self, store, kind, details, **extra):
        resource_id = self.state.new_id(kind)
        resource = FakeModel(
            id=resource_id, lifecycle_state="AVAILABLE", **details.__dict__, **extra
        )
        store[resource_id] = resource
        return Response(resource)

    def list_vcns(self, compartment_id, **kwargs):
        return Response(
            [
                v
                for v in self.state.vcns.values()
                if v.display_name == kwargs.get("display_name")
            ]
        )

    def create_vcn(self, details, **kwargs):
        self.state.calls.append("create_vcn")
        rt = FakeModel(id=self.state.new_id("routetable"), route_rules=[])
        sl = FakeModel(
            id=self.state.new_id("securitylist"),
            ingress_security_rules=[],
            egress_security_rules=[],
        )
        self.state.route_tables[rt.id] = rt
        self.state.security_lists[sl.id] = sl
        return self._create(
            self.state.vcns,
            "vcn",
            details,
            default_route_table_id=rt.id,
            default_security_list_id=sl.id,
        )

    def get_vcn(self, vcn_id, **kwargs):
        return Response(self.state.vcns[vcn_id])

    def list_internet_gateways(self, compartment_id, **kwargs):
        return Response(
            [
                g
                for g in self.state.internet_gateways.values()
                if g.vcn_id == kwargs.get("vcn_id")
            ]
        )

    def create_internet_gateway(self, details, **kwargs):
        self.state.calls.append("create_internet_gateway")
        return self._create(self.state.internet_gateways, "internetgateway", details)

    def get_internet_gateway(self, ig_id, **kwargs):
        return Response(self.state.internet_gateways[ig_id])

    def get_route_table(self, rt_id, **kwargs):
        return Response(self.state.route_tables[rt_id])

    def update_route_table(self, rt_id, details, **kwargs):
        self.state.calls.append("update_route_table")
        self.state.route_tables[rt_id].route_rules = list(details.route_rules)
        return Response(self.state.route_tables[rt_id])

    def get_security_list(self, sl_id, **kwargs):
        return Response(self.state.security_lists[sl_id])

    def update_security_list(self, sl_id, details, **kwargs):
        self.state.calls.append("update_security_list")
        sl = self.state.security_lists[sl_id]
        sl.ingress_security_rules = list(details.ingress_security_rules)
        sl.egress_security_rules = list(details.egress_security_rules)
        return Response(sl)

    def list_subnets(self, compartment_id, **kwargs):
        return Response(
            [
                s
                for s in self.state.subnets.values()
                if s.vcn_id == kwargs.get("vcn_id")
                and s.display_name == kwargs.get("display_name")
            ]
        )

    def create_subnet(self, details, **kwargs):
        self.state.calls.append("create_subnet")
        return self._create(self.state.subnets, "subnet", details)

    def get_subnet(self, subnet_id, **kwargs):
        return Response(self.state.subnets[subnet_id])

    def get_vnic(self, vnic_id, **kwargs):
        return Response(self.state.vnics[vnic_id])


class FakeIdentityClient:
    def __init__(self, config, **kwargs):
        self.region = config.get("region")
        self.state = STATE

    def list_availability_domains(self, compartment_id, **kwargs):
        return Response([FakeModel(name=AD), FakeModel(name="Uocm:PHX-AD-2")])

    def get_tenancy(self, tenancy_id, **kwargs):
        return Response(FakeModel(id=tenancy_id, home_region_key="IAD"))

    def get_compartment(self, compartment_id, **kwargs):
        if self.state.deny_get_compartment:
            raise FakeServiceError(404, "NotAuthorizedOrNotFound", message="denied")
        return Response(FakeModel(id=compartment_id, compartment_id=TENANCY))

    def list_dynamic_groups(self, compartment_id, **kwargs):
        return Response(
            [
                g
                for g in self.state.dynamic_groups.values()
                if g.name == kwargs.get("name")
            ]
        )

    def create_dynamic_group(self, details, **kwargs):
        self.state.calls.append(f"create_dynamic_group@{self.region}")
        if self.state.deny_iam:
            raise FakeServiceError(404, "NotAuthorizedOrNotFound", message="denied")
        group = FakeModel(
            id=self.state.new_id("dynamicgroup"),
            lifecycle_state="ACTIVE",
            **details.__dict__,
        )
        self.state.dynamic_groups[group.id] = group
        return Response(group)

    def list_policies(self, compartment_id, **kwargs):
        return Response(
            [p for p in self.state.policies.values() if p.name == kwargs.get("name")]
        )

    def create_policy(self, details, **kwargs):
        self.state.calls.append(f"create_policy@{self.region}")
        if self.state.deny_iam:
            raise FakeServiceError(404, "NotAuthorizedOrNotFound", message="denied")
        policy = FakeModel(
            id=self.state.new_id("policy"), lifecycle_state="ACTIVE", **details.__dict__
        )
        self.state.policies[policy.id] = policy
        return Response(policy)

    def update_policy(self, policy_id, details, **kwargs):
        self.state.calls.append("update_policy")
        self.state.policies[policy_id].statements = list(details.statements)
        return Response(self.state.policies[policy_id])


class FakeSecurityTokenSigner:
    def __init__(self, token, private_key):
        self.token = token
        self.private_key = private_key


class FakeInstancePrincipalsSigner:
    tenancy_id = TENANCY

    def __init__(self, **kwargs):
        pass


STATE = FakeState()
CONFIG_FILE_CONTENT: Dict[str, Any] = {}


def _from_file(file_location, profile_name="DEFAULT"):
    if profile_name not in CONFIG_FILE_CONTENT:
        raise FakeProfileNotFound(profile_name)
    return dict(CONFIG_FILE_CONTENT[profile_name])


def _build_fake_oci_module():
    oci = types.ModuleType("oci")
    oci.core = types.SimpleNamespace(
        ComputeClient=FakeComputeClient,
        VirtualNetworkClient=FakeNetworkClient,
        models=_Models(),
    )
    oci.identity = types.SimpleNamespace(
        IdentityClient=FakeIdentityClient, models=_Models()
    )
    oci.exceptions = types.SimpleNamespace(
        ServiceError=FakeServiceError, ProfileNotFound=FakeProfileNotFound
    )
    oci.config = types.SimpleNamespace(
        from_file=_from_file, validate_config=lambda cfg: None
    )
    oci.auth = types.SimpleNamespace(
        signers=types.SimpleNamespace(
            SecurityTokenSigner=FakeSecurityTokenSigner,
            InstancePrincipalsSecurityTokenSigner=FakeInstancePrincipalsSigner,
        )
    )
    oci.signer = types.SimpleNamespace(
        load_private_key_from_file=lambda path, pass_phrase=None: f"key:{path}"
    )
    oci.retry = types.SimpleNamespace(DEFAULT_RETRY_STRATEGY="retry")
    oci.pagination = types.SimpleNamespace(
        list_call_get_all_results=lambda fn, *args, **kwargs: fn(*args, **kwargs)
    )
    oci.regions = types.SimpleNamespace(
        REGIONS_SHORT_NAMES={"iad": "us-ashburn-1", "phx": "us-phoenix-1"}
    )
    return oci


@pytest.fixture
def fake_oci(monkeypatch, tmp_path):
    """Install the fake SDK, reset shared state and point HOME at tmp_path."""
    global STATE
    STATE = FakeState()
    CONFIG_FILE_CONTENT.clear()
    config_path = tmp_path / "oci_config"
    config_path.write_text("[DEFAULT]\n")
    CONFIG_FILE_CONTENT["DEFAULT"] = {
        "tenancy": TENANCY,
        "user": "ocid1.user.oc1..u",
        "fingerprint": "aa:bb",
        "key_file": str(tmp_path / "key.pem"),
        "region": "us-ashburn-1",
    }
    monkeypatch.setitem(sys.modules, "oci", _build_fake_oci_module())
    monkeypatch.setenv("HOME", str(tmp_path))
    from ray.autoscaler._private._oci import (
        config as oci_config,
        node_provider as oci_node_provider,
    )

    monkeypatch.setattr(oci_config, "IAM_PROPAGATION_WAIT_S", 0)
    monkeypatch.setattr(oci_node_provider, "CONFLICT_RETRY_INITIAL_DELAY_S", 0)
    monkeypatch.setattr(oci_node_provider, "CONFLICT_RETRY_MAX_DELAY_S", 0)
    STATE.config_path = str(config_path)
    STATE.tmp_path = tmp_path
    yield STATE


def _provider_config(state, **overrides):
    config = {
        "type": "oci",
        "region": REGION,
        "compartment_id": COMPARTMENT,
        "availability_domain": AD,
        "subnet_id": "ocid1.subnet.oc1.phx.existing",
        "oci_config_file": state.config_path,
        "oci_config_profile": "DEFAULT",
    }
    config.update(overrides)
    return config


def _provider(state, **overrides):
    from ray.autoscaler._private._oci.node_provider import OCINodeProvider

    return OCINodeProvider(_provider_config(state, **overrides), "test-cluster")


def _tags(kind=NODE_KIND_WORKER, node_type="cpu_worker", **extra):
    tags = {
        TAG_RAY_NODE_KIND: kind,
        TAG_RAY_USER_NODE_TYPE: node_type,
        TAG_RAY_LAUNCH_CONFIG: "abc123",
        TAG_RAY_NODE_NAME: f"ray-test-cluster-{kind}",
        TAG_RAY_NODE_STATUS: "uninitialized",
    }
    tags.update(extra)
    return tags


def _cluster_tags(**extra):
    return {TAG_RAY_CLUSTER_NAME: "test-cluster", **_tags(**extra)}


def _node_config(**overrides):
    config = {
        "shape": "VM.Standard.E4.Flex",
        "shape_config": {"ocpus": 2, "memory_in_gbs": 16},
        "image_id": "ocid1.image.oc1.phx.img",
        "metadata": {"ssh_authorized_keys": "ssh-rsa AAAA test"},
    }
    config.update(overrides)
    return config


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------


def test_auth_api_key_profile(fake_oci):
    provider = _provider(fake_oci)
    assert provider.client.auth_mode == "api_key"
    assert provider.client.caller_tenancy_id == TENANCY
    # Clients are created for the provider region, not the profile region.
    assert provider.client.compute.region == REGION


def test_auth_session_token_profile(fake_oci):
    token_file = fake_oci.tmp_path / "token"
    token_file.write_text("session-token\n")
    CONFIG_FILE_CONTENT["SESSION"] = {
        "tenancy": TENANCY,
        "fingerprint": "aa:bb",
        "key_file": str(fake_oci.tmp_path / "session_key.pem"),
        "security_token_file": str(token_file),
        "region": "us-ashburn-1",
    }
    provider = _provider(fake_oci, oci_config_profile="SESSION")
    assert provider.client.auth_mode == "security_token"
    signer = provider.client._signer
    assert signer.token == "session-token"
    assert signer.private_key.endswith("session_key.pem")


def test_auth_falls_back_to_instance_principal(fake_oci):
    provider = _provider(
        fake_oci, oci_config_file=str(fake_oci.tmp_path / "does-not-exist")
    )
    assert provider.client.auth_mode == "instance_principal"
    assert provider.client.caller_tenancy_id == TENANCY


def test_auth_missing_credentials_is_a_clear_error(fake_oci):
    with pytest.raises(RuntimeError, match="file_mounts"):
        _provider(
            fake_oci,
            oci_config_file=str(fake_oci.tmp_path / "does-not-exist"),
            use_instance_principal=False,
        )


def test_missing_sdk_error_message(fake_oci, monkeypatch):
    from ray.autoscaler._private.providers import _import_oci

    monkeypatch.setitem(sys.modules, "oci", None)
    with pytest.raises(ImportError, match="pip install oci"):
        _import_oci({"type": "oci"})


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


def test_validate_freeform_tags_limits():
    from ray.autoscaler._private._oci.utils import validate_freeform_tags

    assert validate_freeform_tags({"a": 1}) == {"a": "1"}
    with pytest.raises(ValueError, match="at most 10"):
        validate_freeform_tags({f"k{i}": "v" for i in range(11)})
    with pytest.raises(ValueError, match="exceeds 256"):
        validate_freeform_tags({"k": "v" * 257})
    with pytest.raises(ValueError, match="exceeds 100"):
        validate_freeform_tags({"k" * 101: "v"})


def test_non_terminated_nodes_filters_by_tags_and_state(fake_oci):
    head = fake_oci.add_instance(_cluster_tags(kind=NODE_KIND_HEAD, node_type="head"))
    worker = fake_oci.add_instance(_cluster_tags())
    # A node type may override the AD; such nodes must still be visible even
    # though the provider itself is configured with AD.
    other_ad_worker = fake_oci.add_instance(
        _cluster_tags(node_type="gpu_worker"), ad=AD2
    )
    fake_oci.add_instance(_cluster_tags(), state="TERMINATED")
    fake_oci.add_instance(_cluster_tags(), state="TERMINATING")
    stopped = fake_oci.add_instance(_cluster_tags(), state="STOPPED")
    fake_oci.add_instance({**_cluster_tags(), TAG_RAY_CLUSTER_NAME: "other-cluster"})
    fake_oci.add_instance({"unrelated": "instance"})

    provider = _provider(fake_oci)
    assert sorted(provider.non_terminated_nodes({})) == sorted(
        [head, worker, other_ad_worker]
    )
    assert provider.non_terminated_nodes({TAG_RAY_USER_NODE_TYPE: "gpu_worker"}) == [
        other_ad_worker
    ]
    # Instances are listed compartment-wide, never filtered by AD.
    assert "list_instances:None" in fake_oci.calls
    assert not any(
        c.startswith("list_instances:") and c != "list_instances:None"
        for c in fake_oci.calls
    )
    assert provider.non_terminated_nodes({TAG_RAY_NODE_KIND: NODE_KIND_HEAD}) == [head]
    assert provider.non_terminated_nodes({TAG_RAY_USER_NODE_TYPE: "cpu_worker"}) == [
        worker
    ]
    assert provider.is_running(head)
    assert not provider.is_terminated(head)
    assert provider.is_terminated(stopped)
    assert provider.is_terminated("ocid1.instance.oc1.phx.gone")
    assert provider.node_tags(worker)[TAG_RAY_USER_NODE_TYPE] == "cpu_worker"


def test_set_node_tags_merges_existing_tags(fake_oci):
    node = fake_oci.add_instance(_cluster_tags())
    provider = _provider(fake_oci)
    provider.set_node_tags(node, {TAG_RAY_NODE_STATUS: "up-to-date"})
    tags = fake_oci.instances[node].freeform_tags
    assert tags[TAG_RAY_NODE_STATUS] == "up-to-date"
    assert tags[TAG_RAY_CLUSTER_NAME] == "test-cluster"
    assert fake_oci.calls.count("update_instance") == 1
    # No-op updates do not call the API.
    provider.set_node_tags(node, {TAG_RAY_NODE_STATUS: "up-to-date"})
    assert fake_oci.calls.count("update_instance") == 1


# ---------------------------------------------------------------------------
# Create / terminate
# ---------------------------------------------------------------------------


def test_create_node_launches_tagged_instances(fake_oci):
    provider = _provider(fake_oci)
    created = provider.create_node(
        _node_config(freeform_tags={"team": "ml"}), _tags(), count=2
    )
    assert len(created) == 2
    assert fake_oci.calls.count("launch_instance") == 2
    for instance in created.values():
        details = instance.launch_details
        assert details.compartment_id == COMPARTMENT
        assert details.availability_domain == AD
        assert details.shape == "VM.Standard.E4.Flex"
        assert details.shape_config.ocpus == 2
        assert details.source_details.image_id == "ocid1.image.oc1.phx.img"
        assert details.create_vnic_details.subnet_id == "ocid1.subnet.oc1.phx.existing"
        assert details.create_vnic_details.assign_public_ip is True
        assert details.metadata["ssh_authorized_keys"].startswith("ssh-rsa")
        assert details.display_name == "ray-test-cluster-worker"
        assert details.freeform_tags["team"] == "ml"
        assert details.freeform_tags[TAG_RAY_CLUSTER_NAME] == "test-cluster"
        assert details.freeform_tags[TAG_RAY_NODE_KIND] == NODE_KIND_WORKER
    # Newly launched nodes are visible immediately.
    assert set(provider.non_terminated_nodes({})) == set(created)


def test_create_node_respects_internal_ips_and_node_subnet(fake_oci):
    provider = _provider(fake_oci, use_internal_ips=True)
    created = provider.create_node(
        _node_config(subnet_id="ocid1.subnet.oc1.phx.private"), _tags(), count=1
    )
    details = next(iter(created.values())).launch_details
    assert details.create_vnic_details.subnet_id == "ocid1.subnet.oc1.phx.private"
    assert details.create_vnic_details.assign_public_ip is False


def test_create_node_out_of_capacity(fake_oci):
    fake_oci.launch_error = FakeServiceError(
        500, "InternalError", message="Out of host capacity."
    )
    provider = _provider(fake_oci)
    with pytest.raises(NodeLaunchException) as exc_info:
        provider.create_node(_node_config(), _tags(), count=1)
    assert exc_info.value.category == "OutOfCapacity"


def test_create_node_limit_exceeded(fake_oci):
    fake_oci.launch_error = FakeServiceError(400, "LimitExceeded", message="quota")
    provider = _provider(fake_oci)
    with pytest.raises(NodeLaunchException) as exc_info:
        provider.create_node(_node_config(), _tags(), count=1)
    assert exc_info.value.category == "LimitExceeded"


def test_create_node_rejects_unknown_node_config_key(fake_oci):
    provider = _provider(fake_oci)
    # Make the fake LaunchInstanceDetails strict like the real SDK model.
    models = sys.modules["oci"].core.models

    class StrictLaunchInstanceDetails(FakeModel):
        def __init__(self, **kwargs):
            if "bogus" in kwargs:
                raise TypeError("Unrecognized keyword arguments: bogus")
            super().__init__(**kwargs)

    models._classes["LaunchInstanceDetails"] = StrictLaunchInstanceDetails
    with pytest.raises(ValueError, match="Invalid key in `node_config`"):
        provider.create_node(_node_config(bogus=1), _tags(), count=1)


def test_create_node_requires_image_and_subnet(fake_oci):
    provider = _provider(fake_oci, subnet_id=None)
    with pytest.raises(ValueError, match="No subnet configured"):
        provider.create_node(_node_config(), _tags(), count=1)
    provider = _provider(fake_oci)
    with pytest.raises(ValueError, match="image_id"):
        provider.create_node(_node_config(image_id=None), _tags(), count=1)


def test_conflicts_while_provisioning_are_retried(fake_oci):
    """OCI returns 409 Conflict for updates/terminations of an instance that
    is still being modified (e.g. right after launch); the provider retries."""
    node = fake_oci.add_instance(_cluster_tags(), state="PROVISIONING")
    fake_oci.conflicts = {"update_instance": 2, "terminate_instance": 1}
    provider = _provider(fake_oci)
    provider.set_node_tags(node, {TAG_RAY_NODE_STATUS: "waiting-for-ssh"})
    assert (
        fake_oci.instances[node].freeform_tags[TAG_RAY_NODE_STATUS] == "waiting-for-ssh"
    )
    assert fake_oci.calls.count("update_instance:409") == 2
    provider.terminate_node(node)
    assert fake_oci.instances[node].lifecycle_state == "TERMINATED"
    assert fake_oci.calls.count("terminate_instance:409") == 1


def test_conflict_retry_gives_up_after_deadline(fake_oci, monkeypatch):
    from ray.autoscaler._private._oci import node_provider as oci_node_provider

    monkeypatch.setattr(oci_node_provider, "CONFLICT_RETRY_TIMEOUT_S", 0)
    node = fake_oci.add_instance(_cluster_tags(), state="PROVISIONING")
    fake_oci.conflicts = {"update_instance": 5}
    provider = _provider(fake_oci)
    with pytest.raises(FakeServiceError):
        provider.set_node_tags(node, {TAG_RAY_NODE_STATUS: "waiting-for-ssh"})


def test_terminate_nodes(fake_oci):
    a = fake_oci.add_instance(_cluster_tags())
    b = fake_oci.add_instance(_cluster_tags())
    provider = _provider(fake_oci)
    provider.terminate_nodes([a, b, "ocid1.instance.oc1.phx.gone"])
    assert fake_oci.instances[a].lifecycle_state == "TERMINATED"
    assert fake_oci.instances[b].lifecycle_state == "TERMINATED"
    assert fake_oci.calls.count("terminate_instance:False") == 2
    assert provider.non_terminated_nodes({}) == []


def test_cache_stopped_nodes_skips_stopping_instances(fake_oci):
    """A STOPPING instance is neither waited for nor reused; a fresh node is
    launched instead (the instance becomes eligible once it is STOPPED)."""
    stopping = fake_oci.add_instance(_cluster_tags(), state="STOPPING")
    provider = _provider(fake_oci, cache_stopped_nodes=True)
    created = provider.create_node(_node_config(), _tags(), count=1)
    assert stopping not in created
    assert fake_oci.calls.count("launch_instance") == 1
    assert "instance_action:START" not in fake_oci.calls
    assert fake_oci.instances[stopping].lifecycle_state == "STOPPING"


def test_claim_stopped_nodes_is_exclusive(fake_oci):
    a = fake_oci.add_instance(_cluster_tags(), state="STOPPED")
    b = fake_oci.add_instance(_cluster_tags(), state="STOPPED")
    provider = _provider(fake_oci, cache_stopped_nodes=True)
    first = provider._claim_stopped_nodes(_cluster_tags(), count=1)
    second = provider._claim_stopped_nodes(_cluster_tags(), count=5)
    third = provider._claim_stopped_nodes(_cluster_tags(), count=5)
    assert {inst.id for inst in first} | {inst.id for inst in second} == {a, b}
    assert third == []
    assert provider._claimed_for_reuse == {a, b}


def test_concurrent_create_node_reuses_each_stopped_instance_once(
    fake_oci, monkeypatch
):
    """Two concurrent create_node() calls with one STOPPED instance: exactly
    one caller restarts it, the other launches a new instance."""
    stopped = fake_oci.add_instance(_cluster_tags(), state="STOPPED")
    provider = _provider(fake_oci, cache_stopped_nodes=True)

    start_entered = threading.Event()
    release_start = threading.Event()
    original_action = FakeComputeClient.instance_action

    def slow_start(self, instance_id, action, **kwargs):
        start_entered.set()
        assert release_start.wait(10)
        return original_action(self, instance_id, action, **kwargs)

    monkeypatch.setattr(FakeComputeClient, "instance_action", slow_start)

    results: Dict[str, Dict[str, Any]] = {}

    def run(name):
        results[name] = provider.create_node(_node_config(), _tags(), count=1)

    first = threading.Thread(target=run, args=("first",))
    first.start()
    # The first caller has claimed the instance and is blocked in START (no
    # lock held); the second caller must not pick the same instance.
    assert start_entered.wait(10)
    second = threading.Thread(target=run, args=("second",))
    second.start()
    second.join(10)
    assert not second.is_alive(), "second create_node() must not block on the first"
    release_start.set()
    first.join(10)

    assert list(results["first"]) == [stopped]
    assert stopped not in results["second"]
    assert len(results["second"]) == 1
    assert fake_oci.calls.count("launch_instance") == 1
    assert fake_oci.calls.count("instance_action:START") == 1
    assert provider._claimed_for_reuse == set()


def test_cache_stopped_nodes_stops_and_reuses(fake_oci):
    node = fake_oci.add_instance(_cluster_tags())
    provider = _provider(fake_oci, cache_stopped_nodes=True)
    provider.terminate_node(node)
    assert fake_oci.instances[node].lifecycle_state == "STOPPED"
    assert "instance_action:STOP" in fake_oci.calls
    assert provider.non_terminated_nodes({}) == []
    assert provider.is_terminated(node)

    created = provider.create_node(_node_config(), _tags(), count=2)
    assert node in created
    assert fake_oci.instances[node].lifecycle_state == "RUNNING"
    assert fake_oci.calls.count("launch_instance") == 1


# ---------------------------------------------------------------------------
# IP addresses
# ---------------------------------------------------------------------------


def test_ip_lookup_waits_for_vnic_and_caches(fake_oci):
    node = fake_oci.add_instance(_cluster_tags())
    provider = _provider(fake_oci)
    # No VNIC attached yet: the SSH runner polls until an IP appears.
    assert provider.external_ip(node) is None
    assert provider.internal_ip(node) is None

    fake_oci.attach_vnic(node, "10.77.0.5", "129.146.1.2")
    assert provider.internal_ip(node) == "10.77.0.5"
    assert provider.external_ip(node) == "129.146.1.2"
    # Cached afterwards.
    fake_oci.vnics.clear()
    assert provider.external_ip(node) == "129.146.1.2"
    assert provider.get_node_id("129.146.1.2") == node
    assert provider.get_node_id("10.77.0.5", use_internal_ip=True) == node


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


def _cluster_config(state, **provider_overrides):
    provider = _provider_config(state, **provider_overrides)
    for key in ("subnet_id", "availability_domain"):
        if key not in provider_overrides:
            provider.pop(key, None)
    key = state.tmp_path / "id_rsa"
    key.write_text("PRIVATE")
    (state.tmp_path / "id_rsa.pub").write_text("ssh-rsa AAAAB3 user@host\n")
    return {
        "cluster_name": "test-cluster",
        "provider": provider,
        "auth": {"ssh_user": "ubuntu", "ssh_private_key": str(key)},
        "head_node_type": "ray.head.default",
        "available_node_types": {
            "ray.head.default": {
                "resources": {},
                "node_config": {
                    "shape": "VM.Standard.E4.Flex",
                    "shape_config": {"ocpus": 2, "memory_in_gbs": 16},
                },
            },
            "gpu_worker": {
                "resources": {},
                "min_workers": 0,
                "max_workers": 1,
                "node_config": {"shape": "VM.GPU.A10.1"},
            },
        },
    }


def _add_images(state):
    state.images = [
        FakeModel(
            id="ocid1.image.oc1.phx.old",
            display_name="Canonical-Ubuntu-22.04-2024.01.01-0",
            operating_system="Canonical Ubuntu",
            operating_system_version="22.04",
            time_created=1,
            shapes={"VM.Standard.E4.Flex", "VM.GPU.A10.1"},
        ),
        FakeModel(
            id="ocid1.image.oc1.phx.new",
            display_name="Canonical-Ubuntu-22.04-2024.06.01-0",
            operating_system="Canonical Ubuntu",
            operating_system_version="22.04",
            time_created=2,
            shapes={"VM.Standard.E4.Flex", "VM.GPU.A10.1"},
        ),
        FakeModel(
            id="ocid1.image.oc1.phx.minimal",
            display_name="Canonical-Ubuntu-22.04-Minimal-2024.07.01-0",
            operating_system="Canonical Ubuntu",
            operating_system_version="22.04",
            time_created=3,
            shapes={"VM.Standard.E4.Flex", "VM.GPU.A10.1"},
        ),
    ]


def test_bootstrap_creates_network_iam_and_key_idempotently(fake_oci):
    from ray.autoscaler._private._oci.config import bootstrap_oci

    _add_images(fake_oci)
    config = _cluster_config(fake_oci)
    out = bootstrap_oci(config)

    provider = out["provider"]
    assert provider["availability_domain"] == AD
    subnet = fake_oci.subnets[provider["subnet_id"]]
    assert subnet.display_name == "ray-autoscaler-subnet"
    assert subnet.cidr_block == "10.77.0.0/24"
    assert subnet.prohibit_public_ip_on_vnic is False
    vcn = fake_oci.vcns[subnet.vcn_id]
    assert vcn.display_name == "ray-autoscaler-vcn"
    assert vcn.cidr_blocks == ["10.77.0.0/16"]
    assert vcn.freeform_tags == {"ray-autoscaler": "true"}
    rules = fake_oci.route_tables[vcn.default_route_table_id].route_rules
    assert [r.destination for r in rules] == ["0.0.0.0/0"]
    security_list = fake_oci.security_lists[vcn.default_security_list_id]
    ingress = security_list.ingress_security_rules
    assert {r.source for r in ingress} == {"0.0.0.0/0", "10.77.0.0/16"}
    ssh_rule = next(r for r in ingress if r.source == "0.0.0.0/0")
    assert ssh_rule.tcp_options.destination_port_range.min == 22
    assert len(security_list.egress_security_rules) == 1

    # IAM resources are created in the tenancy home region.
    assert fake_oci.calls.count("create_dynamic_group@us-ashburn-1") == 1
    assert fake_oci.calls.count("create_policy@us-ashburn-1") == 1
    group = next(iter(fake_oci.dynamic_groups.values()))
    assert group.compartment_id == TENANCY
    assert group.matching_rule == f"ALL {{instance.compartment.id = '{COMPARTMENT}'}}"
    policy = next(iter(fake_oci.policies.values()))
    assert policy.compartment_id == COMPARTMENT
    assert any("manage instance-family" in s for s in policy.statements)
    assert all(group.id in s and COMPARTMENT in s for s in policy.statements)
    assert provider["dynamic_group_name"] == group.name

    # SSH key and images per node type.
    for node_type in out["available_node_types"].values():
        node_config = node_type["node_config"]
        assert node_config["subnet_id"] == provider["subnet_id"]
        assert (
            node_config["metadata"]["ssh_authorized_keys"] == "ssh-rsa AAAAB3 user@host"
        )
        # Newest full (non-Minimal) image wins.
        assert node_config["image_id"] == "ocid1.image.oc1.phx.new"
    assert out["auth"]["ssh_private_key"] == str(fake_oci.tmp_path / "id_rsa")
    # The public key path must not leak into `auth`: the head node hashes
    # launch configs with the key files listed there.
    assert "ssh_public_key" not in out["auth"]

    # Second run reuses everything.
    calls_before = list(fake_oci.calls)
    out2 = bootstrap_oci(config)
    new_calls = fake_oci.calls[len(calls_before) :]
    assert not any(c.startswith(("create_", "update_")) for c in new_calls), new_calls
    assert out2["provider"]["subnet_id"] == provider["subnet_id"]


def test_bootstrap_generates_ssh_key_when_missing(fake_oci, monkeypatch):
    from ray.autoscaler._private._oci import config as oci_config

    _add_images(fake_oci)
    monkeypatch.setattr(
        oci_config, "generate_rsa_key_pair", lambda: ("ssh-rsa GENERATED", "PEM")
    )
    config = _cluster_config(fake_oci)
    config["auth"] = {"ssh_user": "ubuntu"}
    out = oci_config.bootstrap_oci(config)
    key_path = out["auth"]["ssh_private_key"]
    assert key_path.endswith(f"ray-autoscaler_oci_{REGION}.pem")
    assert "ssh_public_key" not in out["auth"]
    assert os.path.exists(key_path[: -len(".pem")] + ".pub")
    assert open(key_path).read() == "PEM"
    assert oct(os.stat(key_path).st_mode & 0o777) == "0o600"
    for node_type in out["available_node_types"].values():
        assert (
            node_type["node_config"]["metadata"]["ssh_authorized_keys"]
            == "ssh-rsa GENERATED"
        )


def test_bootstrap_uses_user_public_key_and_drops_it_from_auth(fake_oci):
    from ray.autoscaler._private._oci.config import bootstrap_oci

    _add_images(fake_oci)
    config = _cluster_config(fake_oci)
    pub = fake_oci.tmp_path / "custom.pub"
    pub.write_text("ssh-ed25519 AAAAC3 custom@host\n")
    config["auth"]["ssh_public_key"] = str(pub)
    out = bootstrap_oci(config)
    assert "ssh_public_key" not in out["auth"]
    for node_type in out["available_node_types"].values():
        assert (
            node_type["node_config"]["metadata"]["ssh_authorized_keys"]
            == "ssh-ed25519 AAAAC3 custom@host"
        )


def test_bootstrap_with_existing_subnet_skips_network(fake_oci):
    from ray.autoscaler._private._oci.config import bootstrap_oci

    _add_images(fake_oci)
    config = _cluster_config(fake_oci, subnet_id="ocid1.subnet.oc1.phx.mine")
    out = bootstrap_oci(config)
    assert "create_vcn" not in fake_oci.calls
    for node_type in out["available_node_types"].values():
        assert node_type["node_config"]["subnet_id"] == "ocid1.subnet.oc1.phx.mine"


def test_bootstrap_without_instance_principal_skips_iam(fake_oci):
    from ray.autoscaler._private._oci.config import bootstrap_oci

    _add_images(fake_oci)
    bootstrap_oci(_cluster_config(fake_oci, use_instance_principal=False))
    assert not any(c.startswith("create_dynamic_group") for c in fake_oci.calls)
    bootstrap_oci(_cluster_config(fake_oci, create_iam_resources=False))
    assert not any(c.startswith("create_dynamic_group") for c in fake_oci.calls)


def test_bootstrap_iam_denied_gives_actionable_error(fake_oci):
    from ray.autoscaler._private._oci.config import bootstrap_oci

    _add_images(fake_oci)
    fake_oci.deny_iam = True
    with pytest.raises(click.ClickException) as exc_info:
        bootstrap_oci(_cluster_config(fake_oci))
    message = str(exc_info.value)
    assert "create_iam_resources: false" in message
    assert "use_instance_principal: false" in message


def test_bootstrap_requires_compartment(fake_oci):
    from ray.autoscaler._private._oci.config import bootstrap_oci

    config = _cluster_config(fake_oci)
    del config["provider"]["compartment_id"]
    with pytest.raises(click.ClickException, match="compartment_id"):
        bootstrap_oci(config)


def test_bootstrap_aborts_when_no_image_matches(fake_oci):
    from ray.autoscaler._private._oci.config import bootstrap_oci

    with pytest.raises(click.ClickException, match="Set `image_id`"):
        bootstrap_oci(_cluster_config(fake_oci))


def _add_shapes(state):
    state.shapes = [
        FakeModel(
            shape="VM.Standard.E4.Flex",
            ocpus=1,
            processor_description="AMD EPYC",
            gpus=0,
        ),
        FakeModel(
            shape="VM.Standard.A1.Flex",
            ocpus=1,
            processor_description="3.0 GHz Ampere Altra",
            gpus=0,
        ),
        # GPU shapes are typically offered in a single AD.
        FakeModel(
            shape="VM.GPU.A10.1",
            ocpus=15,
            processor_description="Intel Xeon",
            gpus=1,
            availability_domains={AD2},
        ),
    ]


def test_fillout_resources_uses_each_node_types_availability_domain(fake_oci):
    from ray.autoscaler._private._oci.config import fillout_resources

    _add_shapes(fake_oci)
    config = _cluster_config(fake_oci, availability_domain=AD)
    # The GPU node type runs in AD2, where the A10 shape exists.
    config["available_node_types"]["gpu_worker"]["node_config"][
        "availability_domain"
    ] = AD2
    out = fillout_resources(copy.deepcopy(config))
    resources = {k: v["resources"] for k, v in out["available_node_types"].items()}
    assert resources["ray.head.default"] == {"CPU": 4}
    assert resources["gpu_worker"] == {"CPU": 30, "GPU": 1}
    assert f"list_shapes:{AD}" in fake_oci.calls
    assert f"list_shapes:{AD2}" in fake_oci.calls

    # With no AD configured anywhere, every AD of the region is consulted.
    fake_oci.calls.clear()
    config = _cluster_config(fake_oci)
    out = fillout_resources(copy.deepcopy(config))
    assert out["available_node_types"]["gpu_worker"]["resources"] == {
        "CPU": 30,
        "GPU": 1,
    }
    assert f"list_shapes:{AD}" in fake_oci.calls
    assert f"list_shapes:{AD2}" in fake_oci.calls


def test_fillout_resources_from_shape(fake_oci):
    from ray.autoscaler._private._oci.config import fillout_resources

    _add_shapes(fake_oci)
    for shape in fake_oci.shapes:
        shape.availability_domains = None
    config = _cluster_config(fake_oci)
    config["available_node_types"]["arm_worker"] = {
        "resources": {"custom": 3},
        "node_config": {"shape": "VM.Standard.A1.Flex", "shape_config": {"ocpus": 4}},
    }
    config["available_node_types"]["gpu_worker"]["resources"] = {"CPU": 8}
    out = fillout_resources(copy.deepcopy(config))
    resources = {k: v["resources"] for k, v in out["available_node_types"].items()}
    assert resources["ray.head.default"] == {"CPU": 4}
    assert resources["arm_worker"] == {"CPU": 4, "custom": 3}
    # GPU detected from the shape; the user's CPU override is kept.
    assert resources["gpu_worker"] == {"CPU": 8, "GPU": 1}


def test_tenancy_of_compartment_walks_parents(fake_oci):
    provider = _provider(fake_oci)
    assert provider.client.tenancy_of_compartment(COMPARTMENT) == TENANCY
    assert provider.client.tenancy_of_compartment(TENANCY) == TENANCY


def test_tenancy_of_compartment_falls_back_to_caller_tenancy(fake_oci, caplog):
    """Principals without `inspect compartments` on the ancestors get 404
    from get_compartment; the caller's own tenancy is used instead."""
    fake_oci.deny_get_compartment = True
    provider = _provider(fake_oci)
    with caplog.at_level("WARNING"):
        assert provider.client.tenancy_of_compartment(COMPARTMENT) == TENANCY
    assert "assuming it belongs to the caller's tenancy" in caplog.text

    # Instance principals know their tenancy through the signer.
    provider = _provider(
        fake_oci, oci_config_file=str(fake_oci.tmp_path / "does-not-exist")
    )
    assert provider.client.tenancy_of_compartment(COMPARTMENT) == TENANCY


def test_tenancy_of_compartment_errors_without_any_tenancy(fake_oci):
    fake_oci.deny_get_compartment = True
    CONFIG_FILE_CONTENT["NOTENANCY"] = {
        "user": "ocid1.user.oc1..u",
        "fingerprint": "aa:bb",
        "key_file": str(fake_oci.tmp_path / "key.pem"),
        "region": "us-ashburn-1",
    }
    provider = _provider(fake_oci, oci_config_profile="NOTENANCY")
    with pytest.raises(RuntimeError, match="create_iam_resources: false"):
        provider.client.tenancy_of_compartment(COMPARTMENT)


def test_bootstrap_iam_works_without_compartment_read_access(fake_oci):
    from ray.autoscaler._private._oci.config import bootstrap_oci

    _add_images(fake_oci)
    fake_oci.deny_get_compartment = True
    out = bootstrap_oci(_cluster_config(fake_oci))
    group = next(iter(fake_oci.dynamic_groups.values()))
    assert group.compartment_id == TENANCY
    assert out["provider"]["dynamic_group_name"] == group.name


def test_provider_is_registered():
    from ray.autoscaler._private.providers import (
        _DEFAULT_CONFIGS,
        _NODE_PROVIDERS,
        _PROVIDER_PRETTY_NAMES,
    )

    assert "oci" in _NODE_PROVIDERS
    assert _PROVIDER_PRETTY_NAMES["oci"] == "OCI"
    assert os.path.exists(_DEFAULT_CONFIGS["oci"]())


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
