raylet = "/tmp/ray/session_latest/logs/raylet.err"
result = "result.txt"

from collections import defaultdict

def analyze(filename):
    object_refs = defaultdict(int)
    object_refs_referenced = defaultdict(int)
    object_refs_funcs = defaultdict(set)
    func_cnt = defaultdict(int)
    total_sizes = 0
    total_sizes_referenced_more_than_once = 0
    with open(filename) as f:
        for line in f.readlines():
            if "[GET]" in line:
                tokens = line.strip("\n").split("[GET]")[1].split(" ")
                object_id = tokens[1].split(":")[1]
                size = tokens[2].split(":")[1]
                if len(tokens) >= 4:
                    func = tokens[3].split(":")[1]
                    object_refs_funcs[object_id].add(func)
                    func_cnt[func] += 1
                object_refs[object_id] += int(size)
                object_refs_referenced[object_id] += 1
                total_sizes += int(size)
                if object_refs[object_id] > 1:
                    total_sizes_referenced_more_than_once += int(size)
    # print(filename)
    print(f"total object refs: {len(object_refs)}")
    # for object_id in object_refs.keys():
    #     cnt = object_refs_referenced[object_id]
    #     size = object_refs[object_id]
    #     if cnt > 1:
    #         print(f"{object_id} uses {int(size) /1024 /1024}MB, referenced: {cnt}")
    #         print(f"functions: {object_refs_funcs[object_id]}")
    # print(f"total sizes: {total_sizes / 1024 / 1024} MB")
    # print(f"total sizes referenced more than once: {total_sizes_referenced_more_than_once / 1024 / 1024} MB")
    return object_refs, object_refs_referenced, object_refs_funcs, func_cnt

sizes_dict, _, _, _ = analyze(raylet)
_, ref_count, funcs, func_cnt = analyze(result)

c = 0
func_sizes = defaultdict(int)
for object_id in ref_count.keys():
    cnt = int(ref_count[object_id])
    size = sizes_dict.get(object_id, None)
    if not size:
        continue

    if int(size) > 100 * 1024:
        c += 1
        print(f"{object_id} uses {int(size) /1024 /1024}MB, referenced: {cnt}")
        print(f"functions: {funcs[object_id]}")
        for f in funcs[object_id]:
            func_sizes[f] += (int(size) / 1024 / 1024)
#assert c == len(sizes_dict), len(sizes_dict)
from pprint import pprint
print("function counts:")
pprint(func_cnt)
print("function sizes:")
pprint(func_sizes)
#print(f"total sizes: {total_sizes / 1024 / 1024} MB")
#print(f"total sizes referenced more than once: {total_sizes_referenced_more_than_once / 1024 / 1024} MB")
