class RayServeException(Exception):
    pass


batch_annotation_not_found = RayServeException(
    "max_batch_size is set in config but the function or method does not "
    "accept batching. Please use @serve.accept_batch to explicitly mark "
    "the function or method as batchable and takes in list as arguments.")
