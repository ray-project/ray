# __anti_pattern_start__
import ray
import time
import itertools

ray.init()

numbers = list(range(10000))


def double(number):
    time.sleep(0.00001)
    return number * 2


start_time = time.time()
serial_doubled_numbers = [double(number) for number in numbers]
end_time = time.time()
print(f"Ordinary funciton call takes {end_time - start_time} seconds")
# Ordinary funciton call takes 0.16506004333496094 seconds


@ray.remote
def remote_double(number):
    return double(number)


start_time = time.time()
doubled_number_refs = [remote_double.remote(number) for number in numbers]
parallel_doubled_numbers = ray.get(doubled_number_refs)
end_time = time.time()
print(f"Parallelizing tasks takes {end_time - start_time} seconds")
# Parallelizing tasks takes 1.6061789989471436 seconds
# __anti_pattern_end__

assert serial_doubled_numbers == parallel_doubled_numbers


# __batching_start__
@ray.remote
def remote_double_batch(numbers):
    return [double(number) for number in numbers]


BATCH_SIZE = 1000
start_time = time.time()
doubled_batch_refs = []
for i in range(0, len(numbers), BATCH_SIZE):
    batch = numbers[i : i + BATCH_SIZE]
    doubled_batch_refs.append(remote_double_batch.remote(batch))
parallel_doubled_numbers_with_batching = list(
    itertools.chain(*ray.get(doubled_batch_refs))
)
end_time = time.time()
print(f"Parallelizing tasks with batching takes {end_time - start_time} seconds")
# Parallelizing tasks with batching takes 0.030150890350341797 seconds
# __batching_end__

assert serial_doubled_numbers == parallel_doubled_numbers_with_batching
