import copy
import enum
import random

import ray

LABEL_RESOURCE_AMOUNT = 2 ** 15


@enum.unique
class PlacementStrategy(enum.Enum):
    PACK = 0
    SPREAD = 1


class PlacementGroupHandle(object):
    def __init__(self, name=None, group_id=None, bundles=None):
        self._name = name
        self._ray_group_id = group_id
        self._bundles = bundles if bundles else []

    def get_name(self):
        return self._name

    def get_id(self):
        return self._ray_group_id

    def get_bundles(self):
        return self._bundles


class BundleHandle(object):
    def __init__(self, bundle_id):
        self._ray_bundle_id = bundle_id

    def get_id(self):
        return self._ray_bundle_id


global_pg_table = {}


def alloc_resources(node_info, unit_resources, remaining_cap):
    min_cap = LABEL_RESOURCE_AMOUNT
    for key, demand in unit_resources.items():
        cap = node_info["Resources"][key] / demand
        min_cap = min(min_cap, cap)
        if min_cap == 0:
            break

    alloc_cap = min(min_cap, remaining_cap)
    if alloc_cap > 0:
        for key, demand in unit_resources.items():
            node_info["Resources"][key] -= alloc_cap * demand

    return alloc_cap


def create_placement_group(options):
    group_id = ray.ActorID.from_random()
    group_table = {
        "Id": group_id.hex(),
        "Name": options.name_,
        "Strategy": options.strategy_,
        "Bundles": [],
    }
    global_pg_table[group_id.hex()] = group_table

    raw_nodes = ray.nodes()
    nodes = []
    for raw_node in raw_nodes:
        if not raw_node["Alive"]:
            continue
        node = {
            "NodeID": raw_node["NodeID"],
            "Resources": raw_node["Resources"],
        }
        ray.experimental.set_resource(node["NodeID"], LABEL_RESOURCE_AMOUNT, node["NodeID"])
        nodes.append(node)

    node_count = len(nodes)
    node_index = random.randint(0, node_count)
    for i, bundle in enumerate(options.bundles_):
        bundle_id = ray.ActorID.from_random()
        bundle_table = {
            "Id": bundle_id.hex(),
            "Index": i,
            "ParentId": group_id.hex(),
            "Units": []
        }
        group_table["Bundles"].append(bundle_table)

        # allocate resources one node by one node
        remaining = bundle.unit_count_
        for j in range(0, node_count):
            node = nodes[node_index % node_count]
            allocated = alloc_resources(node, bundle.unit_resources_, remaining)
            for k in range(0, allocated):
                unit = {
                    "AvailableResources": copy.deepcopy(bundle.unit_resources_),
                    "Label": node["NodeID"]
                }
                bundle_table["Units"].append(unit)
            remaining -= allocated
            if remaining == 0:
                break
            node_index += 1

        if remaining > 0:
            raise Exception("There are not enough resources in this cluster.")

        if options.strategy_ == PlacementStrategy.SPREAD:
            node_index += 1

    # construct handles
    bundle_handles = []
    for bundle_table in group_table["Bundles"]:
        bundle_handles.append(BundleHandle(bundle_table["Id"]))
    placement_group_handle = PlacementGroupHandle(group_table["Name"], group_id, bundle_handles)

    return placement_group_handle


class PlacementGroupBuilder(object):
    class BundleBuilder(object):
        def __init__(self, unit_resources=None, unit_count=1):
            self.unit_resources_ = unit_resources if unit_resources else {}
            self.unit_count_ = unit_count

    def __init__(self, name=None, bundles=None, strategy=PlacementStrategy.SPREAD):
        self.name_ = name
        self.bundles_ = bundles if bundles else []
        self.strategy_ = strategy

    def set_name(self, name):
        self.name_ = name
        return self

    def add_bundle(self, resources, count=1):
        self.bundles_.append(PlacementGroupBuilder.BundleBuilder(unit_resources=resources, unit_count=count))
        return self

    def set_strategy(self, strategy):
        self.strategy_ = strategy
        return self

    def create(self):
        return create_placement_group(self)


def placement_group(name=None, bundles=None):
    return PlacementGroupBuilder(name, bundles)
