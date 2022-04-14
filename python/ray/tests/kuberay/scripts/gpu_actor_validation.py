import ray
if __name__ == "__main__":
    ray.init("auto")
    gpu_actor = ray.get_actor("gpu_actor")
    actor_response = ray.get(gpu_actor.where_am_i.remote())
    print(actor_response)
