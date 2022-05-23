from collections import namedtuple
import logging
import json
from numbers import Number

# For compatibility under py2 to consider unicode as str
from typing import Optional

from six import string_types

from ray._private.resource_spec import NODE_ID_PREFIX
from ray.tune import TuneError

logger = logging.getLogger(__name__)


class Resources(
    namedtuple(
        "Resources",
        [
            "cpu",
            "gpu",
            "memory",
            "object_store_memory",
            "extra_cpu",
            "extra_gpu",
            "extra_memory",
            "extra_object_store_memory",
            "custom_resources",
            "extra_custom_resources",
            "has_placement_group",
        ],
    )
):
    """Ray resources required to schedule a trial.

    Parameters:
        cpu: Number of CPUs to allocate to the trial.
        gpu: Number of GPUs to allocate to the trial.
        memory: Memory to reserve for the trial.
        object_store_memory: Object store memory to reserve.
        extra_cpu: Extra CPUs to reserve in case the trial needs to
            launch additional Ray actors that use CPUs.
        extra_gpu: Extra GPUs to reserve in case the trial needs to
            launch additional Ray actors that use GPUs.
        extra_memory: Memory to reserve for the trial launching
            additional Ray actors that use memory.
        extra_object_store_memory: Object store memory to reserve for
            the trial launching additional Ray actors that use object store
            memory.
        custom_resources: Mapping of resource to quantity to allocate
            to the trial.
        extra_custom_resources: Extra custom resources to reserve in
            case the trial needs to launch additional Ray actors that use
            any of these custom resources.
        has_placement_group: Bool indicating if the trial also
            has an associated placement group.

    """

    __slots__ = ()

    def __new__(
        cls,
        cpu: float,
        gpu: float,
        memory: float = 0,
        object_store_memory: float = 0.0,
        extra_cpu: float = 0.0,
        extra_gpu: float = 0.0,
        extra_memory: float = 0.0,
        extra_object_store_memory: float = 0.0,
        custom_resources: Optional[dict] = None,
        extra_custom_resources: Optional[dict] = None,
        has_placement_group: bool = False,
    ):
        custom_resources = custom_resources or {}
        extra_custom_resources = extra_custom_resources or {}
        leftovers = set(custom_resources) ^ set(extra_custom_resources)

        for value in leftovers:
            custom_resources.setdefault(value, 0)
            extra_custom_resources.setdefault(value, 0)

        cpu = round(cpu, 2)
        gpu = round(gpu, 2)
        memory = round(memory, 2)
        object_store_memory = round(object_store_memory, 2)
        extra_cpu = round(extra_cpu, 2)
        extra_gpu = round(extra_gpu, 2)
        extra_memory = round(extra_memory, 2)
        extra_object_store_memory = round(extra_object_store_memory, 2)
        custom_resources = {
            resource: round(value, 2) for resource, value in custom_resources.items()
        }
        extra_custom_resources = {
            resource: round(value, 2)
            for resource, value in extra_custom_resources.items()
        }

        all_values = [
            cpu,
            gpu,
            memory,
            object_store_memory,
            extra_cpu,
            extra_gpu,
            extra_memory,
            extra_object_store_memory,
        ]
        all_values += list(custom_resources.values())
        all_values += list(extra_custom_resources.values())
        assert len(custom_resources) == len(extra_custom_resources)
        for entry in all_values:
            assert isinstance(entry, Number), ("Improper resource value.", entry)
        return super(Resources, cls).__new__(
            cls,
            cpu,
            gpu,
            memory,
            object_store_memory,
            extra_cpu,
            extra_gpu,
            extra_memory,
            extra_object_store_memory,
            custom_resources,
            extra_custom_resources,
            has_placement_group,
        )

    def summary_string(self):
        summary = "{} CPUs, {} GPUs".format(
            self.cpu + self.extra_cpu, self.gpu + self.extra_gpu
        )
        if self.memory or self.extra_memory:
            summary += ", {} GiB heap".format(
                round((self.memory + self.extra_memory) / (1024 ** 3), 2)
            )
        if self.object_store_memory or self.extra_object_store_memory:
            summary += ", {} GiB objects".format(
                round(
                    (self.object_store_memory + self.extra_object_store_memory)
                    / (1024 ** 3),
                    2,
                )
            )
        custom_summary = ", ".join(
            [
                "{} {}".format(self.get_res_total(res), res)
                for res in self.custom_resources
                if not res.startswith(NODE_ID_PREFIX)
            ]
        )
        if custom_summary:
            summary += " ({})".format(custom_summary)
        return summary

    def cpu_total(self):
        return self.cpu + self.extra_cpu

    def gpu_total(self):
        return self.gpu + self.extra_gpu

    def memory_total(self):
        return self.memory + self.extra_memory

    def object_store_memory_total(self):
        return self.object_store_memory + self.extra_object_store_memory

    def get_res_total(self, key):
        return self.custom_resources.get(key, 0) + self.extra_custom_resources.get(
            key, 0
        )

    def get(self, key):
        return self.custom_resources.get(key, 0)

    def is_nonnegative(self):
        all_values = [self.cpu, self.gpu, self.extra_cpu, self.extra_gpu]
        all_values += list(self.custom_resources.values())
        all_values += list(self.extra_custom_resources.values())
        return all(v >= 0 for v in all_values)

    @classmethod
    def subtract(cls, original, to_remove):
        cpu = original.cpu - to_remove.cpu
        gpu = original.gpu - to_remove.gpu
        memory = original.memory - to_remove.memory
        object_store_memory = (
            original.object_store_memory - to_remove.object_store_memory
        )
        extra_cpu = original.extra_cpu - to_remove.extra_cpu
        extra_gpu = original.extra_gpu - to_remove.extra_gpu
        extra_memory = original.extra_memory - to_remove.extra_memory
        extra_object_store_memory = (
            original.extra_object_store_memory - to_remove.extra_object_store_memory
        )
        all_resources = set(original.custom_resources).union(
            set(to_remove.custom_resources)
        )
        new_custom_res = {
            k: original.custom_resources.get(k, 0)
            - to_remove.custom_resources.get(k, 0)
            for k in all_resources
        }
        extra_custom_res = {
            k: original.extra_custom_resources.get(k, 0)
            - to_remove.extra_custom_resources.get(k, 0)
            for k in all_resources
        }
        return Resources(
            cpu,
            gpu,
            memory,
            object_store_memory,
            extra_cpu,
            extra_gpu,
            extra_memory,
            extra_object_store_memory,
            new_custom_res,
            extra_custom_res,
        )

    def to_json(self):
        return resources_to_json(self)


def json_to_resources(data: Optional[str]):
    if data is None or data == "null":
        return None
    if isinstance(data, string_types):
        data = json.loads(data)

    for k in data:
        if k in ["driver_cpu_limit", "driver_gpu_limit"]:
            raise TuneError(
                "The field `{}` is no longer supported. Use `extra_cpu` "
                "or `extra_gpu` instead.".format(k)
            )
        if k not in Resources._fields:
            raise ValueError(
                "Unknown resource field {}, must be one of {}".format(
                    k, Resources._fields
                )
            )
    return Resources(
        data.get("cpu", 1),
        data.get("gpu", 0),
        data.get("memory", 0),
        data.get("object_store_memory", 0),
        data.get("extra_cpu", 0),
        data.get("extra_gpu", 0),
        data.get("extra_memory", 0),
        data.get("extra_object_store_memory", 0),
        data.get("custom_resources"),
        data.get("extra_custom_resources"),
    )


def resources_to_json(resources: Optional[Resources]):
    if resources is None:
        return None
    return {
        "cpu": resources.cpu,
        "gpu": resources.gpu,
        "memory": resources.memory,
        "object_store_memory": resources.object_store_memory,
        "extra_cpu": resources.extra_cpu,
        "extra_gpu": resources.extra_gpu,
        "extra_memory": resources.extra_memory,
        "extra_object_store_memory": resources.extra_object_store_memory,
        "custom_resources": resources.custom_resources.copy(),
        "extra_custom_resources": resources.extra_custom_resources.copy(),
    }
