import ray
ray.init(address="auto")

r = ray.state.state._available_resources_per_node()

pgs = {}
for p, val in ray.util.placement_group_table().items():
    pg_id = val["placement_group_id"]
    state = val["state"]
    strategy = val["strategy"]
    if state != "CREATED":
        continue
    if pg_id not in pgs:
        pgs[pg_id] = {}

from ray.autoscaler._private.util import parse_placement_group_resource_str
total = 0
for k, v in r.items():
    print(f"\nnode: {k}")
    for resource, val in v.items():
        if "group" not in resource:
            print(f"{resource}: {val}")
        else:
            r, id, c = parse_placement_group_resource_str(resource)
            if id not in pgs:
                continue
            if r == "bundle":
                if c:
                    pgs[id]["bundle"] = val
            else:
                if c:
                    pgs[id][r] = val
from pprint import pprint
pprint(len(pgs))
