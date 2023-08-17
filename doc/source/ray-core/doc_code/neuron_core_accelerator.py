# __neuron_core_accelerator_start__
import ray
import os
from ray.util.accelerators import AWS_NEURON_CORE

# On trn1.2xlarge instance, there will be 2 neuron cores.
ray.init(resources={"neuron_cores": 2})


@ray.remote(resources={"neuron_cores": 1})
class NeuronCoreActor:
    def info(self):
        ids = ray.get_runtime_context().get_resource_ids()
        print("neuron_core_ids: {}".format(ids["neuron_cores"]))
        print(f"NEURON_RT_VISIBLE_CORES: {os.environ['NEURON_RT_VISIBLE_CORES']}")


@ray.remote(resources={"neuron_cores": 1}, accelerator_type=AWS_NEURON_CORE)
def use_neuron_core_task():
    ids = ray.get_runtime_context().get_resource_ids()
    print("neuron_core_ids: {}".format(ids["neuron_cores"]))
    print(f"NEURON_RT_VISIBLE_CORES: {os.environ['NEURON_RT_VISIBLE_CORES']}")


neuron_core_actor = NeuronCoreActor.remote()
ray.get(neuron_core_actor.info.remote())
ray.get(use_neuron_core_task.remote())
# __neuron_core_accelerator_end__
