import ray
import numpy as np

import jax
from jax.experimental.pjit import pjit, PartitionSpec as P
from jax.experimental import maps
import jax.numpy as jnp
from jax import pmap
from jax.lax import pmean

from pprint import pprint
import socket
import time

ray.init()

print("this is the host ip address: ", socket.gethostbyname(socket.gethostname()))
host_ip = socket.gethostbyname(socket.gethostname())
host_ip_with_port = host_ip + ":1234"

ip_resources = [x for x in ray.cluster_resources() if "node:" in x]
ip_resources_ = [
    x.replace("node:", "") for x in ray.cluster_resources() if "node:" in x
]

# host node should be zero
ip_resources_ = sorted(ip_resources_, key=lambda x: host_ip not in x)
print(ip_resources_)
ip2hostid_dict = dict(zip(ip_resources_, range(len(ip_resources_))))

pprint(ip2hostid_dict)


# utils
def run_job_on_ray(func):
    results = [
        func.options(resources={ip_resource: 0.01}).remote()
        for ip_resource in ip_resources
    ]
    ray.get(results)


server_addr = host_ip_with_port
num_hosts = len(ip_resources)


def setup_jax_connections():
    ip_addr = socket.gethostbyname(socket.gethostname())
    print(ip_addr)
    host_idx = ip2hostid_dict[ip_addr]
    jax.distributed.initialize(server_addr, num_hosts, host_idx)
    print(
        "host of the node",
        host_ip_with_port,
        "host index",
        host_idx,
        "node ip",
        ip_addr,
    )


@ray.remote(num_cpus=0, num_gpus=4)
def func1():

    setup_jax_connections()

    out = pmap(lambda x: x ** 2)(
        jnp.ones(jax.local_device_count()) * jax.process_index()
    )
    out2 = pmap(lambda x: pmean(x ** 4, axis_name="i"), axis_name="i")(
        jnp.ones(jax.local_device_count()) * jax.process_index()
    )

    print(out)
    print(out2)


@ray.remote(num_cpus=0, num_gpus=4)
def func2():

    setup_jax_connections()

    print(jax.device_count())
    print(jax.local_device_count())

    xs = jnp.ones(jax.local_device_count())
    print(xs)
    xs_sum = jax.pmap(lambda x: jax.lax.psum(x, "i"), axis_name="i")(xs)
    print(xs_sum)

    def f(x, w):
        return jnp.einsum("blm,md->bld", x, w)

    x = jnp.ones((2, 4, 20))
    w = jnp.ones((20, 4))
    print(f(x, w).shape)

    # Model parallelism via pjit
    n = jax.device_count()
    mesh_shape = (n,)
    device_mesh = np.array(jax.devices()).reshape(mesh_shape)
    with maps.Mesh(device_mesh, ("mdl",)):
        result = pjit(
            f,
            in_axis_resources=(P(None, None, "mdl"), P("mdl", None)),
            out_axis_resources=None,
        )(x, w)
        print(result)

    # result is replicated on each chip
    print("print shapes of result on each chip locally")
    for i in range(len(result.device_buffers)):
        print(result.device_buffers[i].shape)

run_job_on_ray(func1)