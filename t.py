import asyncio
import ray

@ray.remote
class A:
    async def gen(self):
        print("BEFORE LOOP:", asyncio.current_task())
        task_name = asyncio.current_task().get_name()
        for i in range(5):
            print("BEFORE YIELD:", asyncio.current_task())
            assert task_name == asyncio.current_task().get_name(), f"{task_name} != {asyncio.current_task().get_name()}"
            yield i
            print("AFTER YIELD:", asyncio.current_task())

        print("AFTER LOOP:", asyncio.current_task())
        assert task_name == asyncio.current_task().get_name()

a = A.remote()
for obj_ref in ray.get(a.gen.options(num_returns="streaming").remote()):
    print(ray.get(obj_ref))