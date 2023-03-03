batch_size_per_worker = 32
global_batch_size = batch_size_per_worker * session.get_world_size()
datasets = get_datasets(global_batch_size, batch_size_per_worker)
