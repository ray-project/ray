import pytest

from ray.experimental.rdt.nixl_tensor_transport import NixlTensorTransport


class FakeStorage:
    def __init__(self, data_ptr, nbytes):
        self.data_ptr_value = data_ptr
        self.nbytes_value = nbytes

    def data_ptr(self):
        return self.data_ptr_value

    def nbytes(self):
        return self.nbytes_value


class FakeDevice:
    type = "cpu"


class FakeTensor:
    is_cuda = False
    dtype = "fake-dtype"
    device = FakeDevice()

    def __init__(self, storage):
        self._storage = storage
        self.shape = (storage.nbytes(),)

    def untyped_storage(self):
        return self._storage

    def get_device(self):
        return -1

    def is_contiguous(self):
        return True


class FakeNixlAgent:
    name = "fake-agent"

    def __init__(self):
        self.calls = []
        self.active = {}
        self.next_registration = 0
        self.fail_next_registration = False
        self.fail_next_deregistration = False

    def register_memory(self, regions, mem_type):
        data_ptr, nbytes, _gpu_id, _metadata = regions[0]
        self.calls.append(("register", data_ptr, nbytes, mem_type))
        if self.fail_next_registration:
            self.fail_next_registration = False
            raise RuntimeError("registration failed")
        self.next_registration += 1
        reg_desc = f"registration-{self.next_registration}"
        self.active[reg_desc] = (data_ptr, nbytes)
        return reg_desc

    def deregister_memory(self, reg_desc):
        self.calls.append(("deregister", reg_desc))
        if self.fail_next_deregistration:
            self.fail_next_deregistration = False
            raise RuntimeError("deregistration failed")
        self.active.pop(reg_desc)

    def get_xfer_descs(self, tensors):
        return [
            (t.untyped_storage().data_ptr(), t.untyped_storage().nbytes())
            for t in tensors
        ]

    def get_serialized_descs(self, xfer_descs):
        return repr(xfer_descs).encode()

    def get_agent_metadata(self):
        return repr(sorted(self.active.values())).encode()


class FakeMemoryPool:
    def __init__(self):
        self.freed = []

    def free_tensors(self, tensors):
        self.freed.extend(tensors)


@pytest.fixture
def transport_and_agent():
    transport = NixlTensorTransport()
    agent = FakeNixlAgent()
    transport._nixl_agent = agent
    transport._backend = "UCX"
    return transport, agent


def test_same_storage_and_extent_reuses_registration(transport_and_agent):
    transport, agent = transport_and_agent
    tensor = FakeTensor(FakeStorage(data_ptr=1000, nbytes=64))

    transport.register_nixl_memory(tensor)
    transport.register_nixl_memory(tensor)

    tensor_desc = transport._tensor_desc_cache[1000]
    assert tensor_desc.metadata_count == 2
    assert [call[0] for call in agent.calls] == ["register"]

    transport.deregister_nixl_memory(tensor)
    assert tensor_desc.metadata_count == 1
    transport.deregister_nixl_memory(tensor)
    assert 1000 not in transport._tensor_desc_cache
    assert [call[0] for call in agent.calls] == ["register", "deregister"]


def test_recycled_pointer_replaces_generation_without_carrying_old_refs(
    transport_and_agent,
):
    transport, agent = transport_and_agent
    old_tensor = FakeTensor(FakeStorage(data_ptr=1000, nbytes=64))
    new_tensor = FakeTensor(FakeStorage(data_ptr=1000, nbytes=128))

    transport.register_nixl_memory(old_tensor)
    transport.register_nixl_memory(old_tensor)
    old_desc = transport._tensor_desc_cache[1000]
    transport.register_nixl_memory(new_tensor)

    new_desc = transport._tensor_desc_cache[1000]
    assert new_desc is not old_desc
    assert new_desc.storage_nbytes == 128
    assert new_desc.metadata_count == 1
    assert agent.calls == [
        ("register", 1000, 64, "cpu"),
        ("deregister", "registration-1"),
        ("register", 1000, 128, "cpu"),
    ]

    # Both old explicit pins release old_desc and cannot touch new_desc.
    transport.deregister_nixl_memory(old_tensor)
    transport.deregister_nixl_memory(old_tensor)
    assert transport._tensor_desc_cache[1000] is new_desc
    assert new_desc.metadata_count == 1

    transport.deregister_nixl_memory(new_tensor)
    assert 1000 not in transport._tensor_desc_cache


def test_same_size_recycled_pointer_is_not_a_storage_cache_hit(transport_and_agent):
    transport, agent = transport_and_agent
    old_tensor = FakeTensor(FakeStorage(data_ptr=1000, nbytes=64))
    new_tensor = FakeTensor(FakeStorage(data_ptr=1000, nbytes=64))
    transport.register_nixl_memory(old_tensor)
    old_desc = transport._tensor_desc_cache[1000]

    transport.register_nixl_memory(new_tensor)

    new_desc = transport._tensor_desc_cache[1000]
    assert new_desc is not old_desc
    assert new_desc.metadata_count == 1
    assert [call[0] for call in agent.calls] == [
        "register",
        "deregister",
        "register",
    ]

    transport.deregister_nixl_memory(old_tensor)
    assert transport._tensor_desc_cache[1000] is new_desc


def test_in_place_resize_preserves_same_storage_owners(transport_and_agent):
    transport, agent = transport_and_agent
    storage = FakeStorage(data_ptr=1000, nbytes=64)
    tensor = FakeTensor(storage)
    transport.register_nixl_memory(tensor)
    tensor_desc = transport._tensor_desc_cache[1000]

    storage.data_ptr_value = 2000
    storage.nbytes_value = 128
    transport.register_nixl_memory(tensor)

    assert 1000 not in transport._tensor_desc_cache
    assert transport._tensor_desc_cache[2000] is tensor_desc
    assert tensor_desc.storage_nbytes == 128
    assert tensor_desc.metadata_count == 2
    assert [call[0] for call in agent.calls] == [
        "register",
        "deregister",
        "register",
    ]

    transport.deregister_nixl_memory(tensor)
    transport.deregister_nixl_memory(tensor)
    assert 2000 not in transport._tensor_desc_cache


def test_replacement_deregistration_failure_is_retryable(transport_and_agent):
    transport, agent = transport_and_agent
    old_tensor = FakeTensor(FakeStorage(data_ptr=1000, nbytes=64))
    new_tensor = FakeTensor(FakeStorage(data_ptr=1000, nbytes=128))
    transport.register_nixl_memory(old_tensor)
    old_desc = transport._tensor_desc_cache[1000]

    agent.fail_next_deregistration = True
    with pytest.raises(RuntimeError, match="deregistration failed"):
        transport.register_nixl_memory(new_tensor)

    assert transport._tensor_desc_cache[1000] is old_desc
    assert old_desc.metadata_count == 1
    assert transport._nixl_agent_meta_version == 0

    transport.register_nixl_memory(new_tensor)
    assert transport._tensor_desc_cache[1000].storage_nbytes == 128
    assert transport._nixl_agent_meta_version == 1


def test_registration_failure_after_replacement_leaves_truthful_generation(
    transport_and_agent,
):
    transport, agent = transport_and_agent
    old_tensor = FakeTensor(FakeStorage(data_ptr=1000, nbytes=64))
    new_tensor = FakeTensor(FakeStorage(data_ptr=1000, nbytes=128))
    transport.register_nixl_memory(old_tensor)

    agent.fail_next_registration = True
    with pytest.raises(RuntimeError, match="Failed to register cpu memory"):
        transport.register_nixl_memory(new_tensor)

    assert 1000 not in transport._tensor_desc_cache
    assert transport._nixl_agent_meta_version == 1
    assert not agent.active

    transport.register_nixl_memory(new_tensor)
    new_desc = transport._tensor_desc_cache[1000]
    transport.deregister_nixl_memory(old_tensor)
    assert transport._tensor_desc_cache[1000] is new_desc
    assert new_desc.metadata_count == 1

    transport.deregister_nixl_memory(new_tensor)
    assert 1000 not in transport._tensor_desc_cache
    assert transport._nixl_agent_meta_version == 2


def test_metadata_after_replacement_contains_new_extent(transport_and_agent):
    transport, _agent = transport_and_agent
    old_tensor = FakeTensor(FakeStorage(data_ptr=1000, nbytes=64))
    new_tensor = FakeTensor(FakeStorage(data_ptr=1000, nbytes=128))
    transport.register_nixl_memory(old_tensor)
    transport.register_nixl_memory(new_tensor)

    metadata = transport.extract_tensor_transport_metadata("new-object", [new_tensor])

    assert metadata.nixl_agent_meta_version == 1
    assert metadata.nixl_agent_meta == b"[(1000, 128)]"
    transport.garbage_collect("new-object", metadata, [new_tensor])
    assert transport._tensor_desc_cache[1000].metadata_count == 1


def test_old_object_metadata_cleanup_cannot_release_new_generation(
    transport_and_agent,
):
    transport, _agent = transport_and_agent
    old_tensor = FakeTensor(FakeStorage(data_ptr=1000, nbytes=64))
    new_tensor = FakeTensor(FakeStorage(data_ptr=1000, nbytes=128))
    old_metadata = transport.extract_tensor_transport_metadata(
        "old-object", [old_tensor]
    )
    old_desc = transport._tensor_desc_cache[1000]

    transport.register_nixl_memory(new_tensor)
    new_desc = transport._tensor_desc_cache[1000]
    assert new_desc is not old_desc

    transport.garbage_collect("old-object", old_metadata, [old_tensor])

    assert transport._tensor_desc_cache[1000] is new_desc
    assert new_desc.metadata_count == 1


def test_pool_descriptor_reference_cleanup_does_not_deregister(
    transport_and_agent,
):
    transport, agent = transport_and_agent
    tensor = FakeTensor(FakeStorage(data_ptr=1000, nbytes=64))
    pool = FakeMemoryPool()
    transport._memory_pool = pool

    refs = transport._add_pool_tensor_descs([tensor])
    transport._remove_tensor_descs([tensor], refs)

    assert 1000 not in transport._tensor_desc_cache
    assert pool.freed == [tensor]
    assert agent.calls == []
    assert transport._nixl_agent_meta_version == 0


def test_explicit_pin_preserves_pool_backed_descriptor(transport_and_agent):
    transport, agent = transport_and_agent
    tensor = FakeTensor(FakeStorage(data_ptr=1000, nbytes=64))
    pool = FakeMemoryPool()
    transport._memory_pool = pool
    refs = transport._add_pool_tensor_descs([tensor])
    pool_desc = refs[0]

    transport.register_nixl_memory(tensor)

    assert transport._tensor_desc_cache[1000] is pool_desc
    assert pool_desc.metadata_count == 2
    assert agent.calls == []

    transport.deregister_nixl_memory(tensor)
    assert pool_desc.metadata_count == 1
    transport._remove_tensor_descs([tensor], refs)
    assert pool.freed == [tensor]


def test_ordinary_deregistration_failure_keeps_last_reference(
    transport_and_agent,
):
    transport, agent = transport_and_agent
    tensor = FakeTensor(FakeStorage(data_ptr=1000, nbytes=64))
    refs = transport._add_tensor_descs([tensor])
    tensor_desc = refs[0]

    agent.fail_next_deregistration = True
    with pytest.raises(RuntimeError, match="deregistration failed"):
        transport._remove_tensor_descs([tensor], refs)

    assert transport._tensor_desc_cache[1000] is tensor_desc
    assert tensor_desc.metadata_count == 1
    assert transport._nixl_agent_meta_version == 0

    transport._remove_tensor_descs([tensor], refs)
    assert 1000 not in transport._tensor_desc_cache
    assert transport._nixl_agent_meta_version == 1
