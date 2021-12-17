import ray


# Mocks, so that this code actually runs.
input_stream = range(100)


def process(inp: int):
    return inp ** 2


# ----- Current Ray -----
# Example taken from:
# https://www.anyscale.com/blog/the-third-generation-of-production-ml-architectures


@ray.remote
class Model:
    def __init__(self, next_actor):
        self.next = next_actor

    def runmodel(self, inp):
        out = process(inp)
        # different for each stage
        self.next.call.remote(out)


# input_stream -> object_detector ->
# object_tracker -> speed_calculator -> result_collector
result_collector = Model.remote()
speed_calculator = Model.remote(next_actor=result_collector)
object_tracker = Model.remote(next_actor=speed_calculator)
object_detector = Model.remote(next_actor=object_tracker)

refs = []
for inp in input_stream:
    refs.append(object_detector.runmodel.remote(inp))
print(ray.get(refs))


# ----- This prototype -----


@ray.remote
async def detect_objects(inp):
    return process(inp)


@ray.remote
async def track_objects(inp):
    return process(inp)


@ray.remote
async def calculate_speeds(inp):
    return process(inp)


@ray.remote
async def collect_results(inp):
    return process(inp)


@ray.remote
async def runmodel(inp):
    objects = await detect_objects(inp)
    tracks = await track_objects(objects)
    speeds = await calculate_speeds(tracks)
    return await collect_results(speeds)


refs = []
for inp in input_stream:
    refs.append(runmodel.remote(inp))
print(ray.get(refs))
