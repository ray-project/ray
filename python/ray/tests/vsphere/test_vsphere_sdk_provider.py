import pytest

from unittest.mock import MagicMock, patch
from ray.autoscaler._private.vsphere.vsphere_sdk_provider import (
    VsphereSdkProvider,
    Constants,
    HardPower,
)


@pytest.fixture
def patch_vsphere_sdk_provider(request):
    patcher = patch(
        "ray.autoscaler._private.vsphere.vsphere_sdk_provider.\
VsphereSdkProvider.__wrapped__.get_client",
        lambda *args, **kwargs: MagicMock(),
    )
    mock_function = patcher.start()

    def fin():
        patcher.stop()

    request.addfinalizer(fin)
    return mock_function


def mock_vsphere_sdk_provider():
    sdk_provider = VsphereSdkProvider("", "", "", Constants.SessionType.UNVERIFIED)
    return sdk_provider


def test_delete_vm_by_id(patch_vsphere_sdk_provider):
    vsp = mock_vsphere_sdk_provider()

    vsp.vsphere_sdk_client.vcenter.vm.Power.stop = MagicMock()

    vsp.vsphere_sdk_client.vcenter.vm.Power.get.return_value = HardPower.Info(
        state=HardPower.State.POWERED_ON
    )
    vsp.delete_vm_by_id("vm2")
    vsp.vsphere_sdk_client.vcenter.vm.Power.stop.assert_called_once_with("vm2")
    vsp.vsphere_sdk_client.vcenter.VM.delete.assert_called_once_with("vm2")


def test_non_terminated_nodes_returns_no_node(patch_vsphere_sdk_provider):
    """There is no node in vSphere"""
    cluster_name = "test"
    vsp = mock_vsphere_sdk_provider()
    vsp.vsphere_sdk_client.vcenter.VM.list.return_value = []
    nodes, tag_cache = vsp.non_terminated_nodes(cluster_name, {})  # assert
    assert len(nodes) == 0

    return


def test_non_terminated_nodes_returns_nodes_in_powered_off_creating_state(
    patch_vsphere_sdk_provider,
):
    cluster_name = "test"
    vsp = mock_vsphere_sdk_provider()

    vsp.vsphere_sdk_client.vcenter.VM.list.return_value = [
        MagicMock(vm="vm1"),
        MagicMock(vm="vm2"),
    ]
    vsp.vsphere_sdk_client.vcenter.vm.Power.get.side_effect = [
        MagicMock(state="POWERED_ON"),
        MagicMock(state="POWERED_OFF"),
    ]
    # The 2nd vm is powered off but is in creating status
    vsp.get_matched_tags = MagicMock(
        return_value=(
            {"ray-cluster-name": "test"},
            {
                "ray-cluster-name": "test",
                "custom-tag": "custom-value",
                "vsphere-node-status": "creating",
            },
        )
    )
    # The tag filter is none
    nodes, tag_cache = vsp.non_terminated_nodes(cluster_name, {})
    assert len(nodes) == 2


def test_non_terminated_nodes_with_custom_tag_filters(patch_vsphere_sdk_provider):
    """Test nodes with custom tag filters"""
    cluster_name = "test"
    vsp = mock_vsphere_sdk_provider()

    vsp.vsphere_sdk_client.vcenter.VM.list.return_value = [
        MagicMock(vm="vm1"),
        MagicMock(vm="vm2"),
    ]
    vsp.vsphere_sdk_client.vcenter.vm.Power.get.side_effect = [
        MagicMock(state="POWERED_ON"),
        MagicMock(state="POWERED_OFF"),
    ]
    vsp.get_matched_tags = MagicMock(
        return_value=(
            {"ray-cluster-name": "test", "custom-tag": "custom-value"},
            {
                "ray-cluster-name": "test",
                "custom-tag": "custom-value",
                "vsphere-node-status": "blabla",
            },
        )
    )
    # This time we applied a tag filter, but the 2nd vm is not in creating
    # status so only the first vm will be returned
    nodes, tag_cache = vsp.non_terminated_nodes(
        cluster_name, {"custom-tag": "custom-value"}
    )  # assert
    assert len(nodes) == 1
    assert nodes[0] == "vm1"


def test_non_terminated_nodes_with_multiple_filters_not_matching(
    patch_vsphere_sdk_provider,
):
    """Test nodes with tag filters not matching"""
    cluster_name = "test"
    vsp = mock_vsphere_sdk_provider()

    vsp.vsphere_sdk_client.vcenter.VM.list.return_value = [
        MagicMock(vm="vm1"),
        MagicMock(vm="vm2"),
    ]
    vsp.vsphere_sdk_client.vcenter.vm.Power.get.side_effect = [
        MagicMock(state="POWERED_ON"),
        MagicMock(state="POWERED_OFF"),
    ]
    vsp.get_matched_tags = MagicMock(
        return_value=(
            {"ray-cluster-name": "test"},
            {"ray-cluster-name": "test", "vsphere-node-status": "blabla"},
        )
    )
    # tag not much so no VM should be returned this time
    nodes, tag_cache = vsp.non_terminated_nodes(
        cluster_name, {"custom-tag": "another-value"}
    )
    assert len(nodes) == 0


def test_get_tag_id_by_name(patch_vsphere_sdk_provider):
    vsp = mock_vsphere_sdk_provider()
    mock_tag = MagicMock()
    mock_tag.name = "ray-node-name"
    vsp.vsphere_sdk_client.tagging.Tag.list_tags_for_category.return_value = ["tag_1"]
    vsp.vsphere_sdk_client.tagging.Tag.get.return_value = mock_tag
    assert vsp.get_tag_id_by_name("ray-node-name", "test_category_id") == "tag_1"


def test_get_tag_id_by_name_return_none(patch_vsphere_sdk_provider):
    vsp = mock_vsphere_sdk_provider()
    mock_tag = MagicMock()
    mock_tag.name = "ray-node-name"
    vsp.vsphere_sdk_client.tagging.Tag.list_tags_for_category.return_value = ["tag_1"]
    vsp.vsphere_sdk_client.tagging.Tag.get.return_value = mock_tag
    assert vsp.get_tag_id_by_name("ray-node-name1", "test_category_id") is None


def test_create_node_tag(patch_vsphere_sdk_provider):
    vsp = mock_vsphere_sdk_provider()
    vsp.vsphere_sdk_client.tagging.Tag.CreateSpec.return_value = "tag_spec"
    vsp.vsphere_sdk_client.tagging.Tag.create.return_value = "tag_id_1"
    tag_id = vsp.create_node_tag("ray_node_tag", "test_category_id")
    assert tag_id == "tag_id_1"


def test_get_category(patch_vsphere_sdk_provider):
    vsp = mock_vsphere_sdk_provider()
    vsp.vsphere_sdk_client.tagging.Category.list.return_value = ["category_1"]

    mock_category = MagicMock()
    mock_category.name = "ray"
    vsp.vsphere_sdk_client.tagging.Category.get.return_value = mock_category
    category = vsp.get_category()
    assert category == "category_1"
    mock_category.name = "default"
    category = vsp.get_category()
    assert category is None


def test_create_category(patch_vsphere_sdk_provider):
    vsp = mock_vsphere_sdk_provider()

    vsp.vsphere_sdk_client.tagging.Category.CreateSpec.return_value = "category_spec"
    vsp.vsphere_sdk_client.tagging.Category.create.return_value = "category_id_1"
    category_id = vsp.create_category()
    assert category_id == "category_id_1"


def test_set_node_tags(patch_vsphere_sdk_provider):
    vsp = mock_vsphere_sdk_provider()
    vsp.vsphere_sdk_client.vcenter.vm.Power.get.return_value = HardPower.Info(
        state=HardPower.State.POWERED_OFF
    )
    vsp.vsphere_sdk_client.vcenter.vm.Power.start.return_value = None

    vsp.get_category = MagicMock(return_value=None)
    vsp.create_category = MagicMock(return_value="category_id")
    vsp.get_tag_id_by_name = MagicMock(return_value=None)
    vsp.create_node_tag = MagicMock(return_value="tag_id")
    vsp.attach_tag = MagicMock(return_value=None)
    vsp.remove_tag_from_vm = MagicMock(return_value=None)

    k = Constants.VSPHERE_NODE_STATUS
    v = Constants.VsphereNodeStatus.CREATED.value
    vsphere_node_created_tag = {k: v}
    vm_id = "vm-test"

    vsp.set_node_tags(vm_id, vsphere_node_created_tag)

    vsp.create_category.assert_called()
    vsp.attach_tag.assert_called_with(vm_id, "VirtualMachine", tag_id="tag_id")
    vsp.attach_tag.assert_called()
