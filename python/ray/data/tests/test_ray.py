import ray
import time

def map1(n):
    time.sleep(1)

    return n

def map2(n):
    time.sleep(1)
    return n

def column_udf_class(col, udf):
    class UDFClass:
        def __call__(self, row):
            return {col: udf(row[col])}
    return UDFClass


if __name__ == '__main__':
    cluster_obj_store_mem = 1000 * 1024 * 1024
    ray.init(num_cpus=5, object_store_memory=cluster_obj_store_mem)
    ray.data.range(1000).map_batches(
        column_udf_class("id", map1),
        concurrency=(1, 4),
    ).map_batches(
        column_udf_class("id", map2),
        concurrency=(1, 4),
    ).show()