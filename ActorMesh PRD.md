# ActorGroup PRD (v0)

[Philipp Moritz](mailto:pcmoritz@anyscale.com)

This document defines a GPU-centric ActorGroup abstraction. It is designed to incorporate the special needs of GPU workloads, but can also be used for CPU workloads.

We realize that the concept of an “actor group” is extremely broad and we won’t be able to make everybody happy. Our focus will be on post-training workloads (which includes inference and training) and the actor group defined here will be somewhat opinionated but also flexible. Anything not covered by the actor groups in this document can always be achieved by using actors directly.

All participating actors in an actor group will have the same class / type (and therefore the same methods) – that part is pretty fundamental since one of the goals of this doc is to make it easier and more efficient to run a task in SPMD style on a group of actors. Furthermore, they each have the same resource requirements, which is less fundamental and could be lifted.

**Tentative Requirements:**

* Support collocating / multiplexing actor groups on the same physical resources (e.g. an actor group for inference and a group for training in RL)  
* Hide the underlying placement group abstraction since it is a more complex / advanced concept, but also power users should be able to configure it to have full control over everything.  
* Allow exposing user defined ranks to the different workers (e.g. data parallel, sequence parallel, tensor parallel, etc). Could alternatively be done by using hierarchical actors like vllm is handled in Ray Data (Ray Data handles the data parallelism, vllm the other types of parallelism).  
* Allow very flexibly to scatter and collect data from the actor group. This can be done using either of two means:  
1) By providing a flexible dispatching mechanism like verl (“Option 1”)  
2) By asking users to wrap the ActorGroup in their own class and handle their data types, like NemoRL does (“Option 2”)

  	We flesh out both options below, but prefer Option 1\.

* (Future) Allow optimizing the underlying actor communication on GPUs, e.g. allow fast collective communication between the actors of the actor group  
* (Future) Allow autoscaling the actor group. This only makes sense in certain settings (e.g. along the data parallel dimension), but as far as possible we try to design the API in a way that it could be added in the future.

**Design decisions:**

* Should the concept of a node be exposed? Right now it is not, but a decent number of the actor groups that exist in libraries do it. It seems unnecessary to do because it can always be emulated by requesting resource bundles that contain all the resources a node has to offer (this of course gets more complex with heterogenous node shapes, in which case we would need to support heterogenous resource bundles). Also, exposing the concept of a node makes the code less portable which is undesirable.  
* Should it be possible to create an actor group from an existing set of actors (e.g. POSIX process groups support this)? We decided not to allow it. While it supports more flexibility (e.g. full flexibility in how to use placement groups to schedule the actors), it has some undesirable properties:  
  * It allows actors of different actor classes to coexist in the same actor group, which wouldn’t make much sense in the context of this proposal, since we want to enable collective invocation of actor methods, which is only possible if all actors have the same type.  
  * It would make it impossible / clunky to retrofit autoscaling into the actor group, which is something we should not rule out for the design.

**Some existing actor group implementations in Ray or other systems:**

1. [Verl](https://github.com/volcengine/verl/blob/main/verl/single_controller/base/worker_group.py), see also [single controller docs](https://verl.readthedocs.io/en/latest/single_controller.html): This implementation is very flexible in terms of how data is scattered to the participating actors and how the results are collected at the end (via a dispatch mechanism). It also supports multiplexing actor groups on underlying resources.  
2. [NemoRL](https://github.com/NVIDIA-NeMo/RL/blob/e600069497270442936df015f37185bfc658b39b/nemo_rl/distributed/worker_groups.py): This implementation looks more like a traditional SPMD shaped computation, and any more intricate processing would need to be done with a wrapper class of the actor group.  
3. [SkyRL](https://github.com/NovaSky-AI/SkyRL/blob/main/skyrl-train/skyrl_train/workers/worker.py#L385) (adapted from [OpenReasonerZero](https://github.com/Open-Reasoner-Zero/Open-Reasoner-Zero))  
4. [Ray Train](https://github.com/ray-project/ray/blob/d0091f7590b0c1a8cf43c54158f8271d8e75b2c1/python/ray/train/_internal/worker_group.py#L102)  
5. [Ray Data](https://github.com/ray-project/ray/blob/57125ad231f0ee6c9fd7c869c809c07a5b5ebd68/python/ray/data/_internal/execution/operators/actor_pool_map_operator.py#L668)  
6. Older Ray [actor group](https://github.com/ray-project/ray/blob/master/python/ray/util/actor_group.py) and [actor pool](https://github.com/ray-project/ray/blob/master/python/ray/util/actor_pool.py) – they mainly differ in that for the actor pool, the user is responsible for creating the actors and for the actor group, the actors are managed by the actor group  
7. [Python Multiprocessing](https://docs.python.org/3/library/multiprocessing.html)  
8. [POSIX Process groups](https://en.wikipedia.org/wiki/Process_group): A little different since they mainly exist for control purposes (e.g. killing all processes at the same time), whereas our actor groups are supposed to also make sending and receiving data from actors more efficient.

# Basic ActorGroup API

```py

# Could also put the extra arguments into .options(), and use *actor_args
# and **actor_kwargs instead
class ActorGroup:
    def __init__(
        actor_cls: Type,
        actor_args,
        actor_kwargs,
        shape: Tuple[int],
        resources_per_actor: Dict[str, float],
        runtime_env=None,
        shape_names: Optional[Tuple[str]],
        placement_group: Optional[PlacementGroup] = None):
        pass

    @property
    def actors(self) -> List[ActorHandle]:
        "Direct access to the underlying actors."
        pass

    @property
    def ranks(self) -> List[Dict[str, Any]]:
        "Rank information for each of the underlying actors."
        pass

    def num_actors(self) -> int:
        # Return the total number of actors in this group

```

Usage:

```py
# Allocate 16 "bundles" of 4 GPUs each (they could for example be made available as 8 nodes of 8 GPUs each)
placement_group = ray.util.placement_group([{"CPU": 4, "GPU": 4}] * 16)

# Create two actor groups on these GPUs, one for inference, one for training
trainer_group = ActorGroup(
    Trainer,
    (model_config,),
    {},
    (16, 2, 2),
    {"CPU": 1, "GPU": 1},
    shape_names=("dp", "cp", "tp"),
    placement_group=placement_group,
)

inference_group = ActorGroup(
    RolloutGenerator,
    (model_config,),
    {},
    (16, 4),
    {"CPU": 1, "GPU": 1},
    shape_names=("dp", "tp"),
    placement_group=placement_group,
)

# Then for example an actor in the training_group would have the following metadata:

assert training_group.ranks[0] == {
    "actor_index": 0,
    "bundle_index": 0,
    "rank": {
        "dp": 0,
        "cp": 0,
        "tp": 0,
    }
}
```

And then there are two different possibilities on how we could structure the APIs to invoke these actors:

# Option 1: ActorGroup as “single controller”

We prefer this solution because the dispatch\_fn below can do all the heavy lifting in terms of distributing the data to the different actors of the mesh.

```py
T = TypeVar("T")

class ActorGroup(Generic[T]):
    # the beginning is like in the ActorGroup class at the beginning

    @property
    def methods(self) -> type[T]:
       # Return handle to the methods of the ActorGroup

    @property
    def shape(self) -> Tuple[int]:
        pass

    @property
    def shape_names(self) -> Tuple[str]:
        pass
        
```

Usage is like the following:

```py
class RolloutGenerator:

    def __init__(self, model_id: str):
        # Instantiate a model with model_id here

    @ray.method
    def generate_sequences(prompts: torch.Tensor) -> torch.Tensor:
        # Invoke the model on the prompts and return the results

    @ray.method(dispatch_fn=my_dispatch_fn, collect_fn=my_collect_fn)
    def generate_custom_sequence(prompts: MyCustomTensor) -> MyCustomTensor:
        # Invoke the model on the prompts and return the results

# Here is an example for custom dispatch and collect functions (we also have defaults for e.g. torch tensors)

def my_dispatch_fn(actor_group: ActorGroup, *args, **kwargs):
    # Split the custom tensors in some custom way
    splitted_args, splitted_kwargs = my_split_function(
        actor_group.world_size,
        *args,
        **kwargs
    )
    return splitted_args, splitted_kwargs

def my_collect_fn(actor_group, output: List):
    # Combine the custom tensors in some custom way
    return my_concat_function(output)

# Here is an example of how the API would be used:

rollout_generator = ActorGroup(RolloutGenerator, ("Qwen/Qwen3-8B"))
torch_results = rollout_generator.methods.generate_sequences(torch_prompts)
my_results = rollout_generator.methods.generate_custom_sequence(my_prompts)
```

# Option 2: ActorGroup as multiprocessing pool

This is more of a SPMD “map” like API. 

```py
class ActorGroup:
    # the beginning is like in the ActorGroup class at the beginning

    # This is NemoRL's run_all_workers_multiple_data
    def map(self, method_name: str, data: List[Any]) -> List[ObjectRef]:
        pass

    # This is NemoRL's run_all_workers_single_data
    def run_all(self, method_name: str, *args, **kwargs) -> List[ObjectRef]:
        pass

    # This is NemoRL's run_single_worker_single_data. In Python multiprocessing
    # terminology, this is called "apply" but maybe that's not
    # descriptive enough
    def run_single(self, method_name: str, *args, **kwargs) -> ObjectRef:
        pass

    # This is NemoRL's run_all_workers_sharded_data
    def run_sharded(
        self,
        method_name: str,
        data: List[Any],
        in_sharded_axes: Optional[list[str]] = None,
        replicate_on_axes: Optional[list[str]] = None,
        output_is_replicated: Opational[list[str]] = None,
    ) -> List[ObjectRef]:
        """
            in_sharded_axes: List of axes that are sharded
            replicate_on_axes: List of axes that are to be replicated
            output_is_replicated: List of axes along which the output is
                replicated (and we should just return the first result).
        """
        pass
    
```

Usage is like the following:

```py
class RolloutGenerator:

    def __init__(self, model_id: str):
        # Instantiate a model with model_id here

    def generate_sequences(prompts: torch.Tensor) -> torch.Tensor:
        # Invoke the model on the prompts and return the results

rollout_generator = ActorGroup(RolloutGenerator, ("Qwen/Qwen3-8B"))
result_refs = rollout_generator.map("generate_sequences", torch_tensor_list)
results = ray.get(result_refs)

# Using the training_group from above
futures = training_group.run_sharded(
    "get_policy_logprobs",
    data=sharded_data_2d,
    in_sharded_axes=["data_parallel", "context_parallel"],
    replicate_on_axes=["tensor_parallel"],
    output_is_replicated=["tensor_parallel"],
)

```

If we go with this API, people would likely create wrapper classes for ActorGroup that shards and combines the data appropriately.

# Some learnings from SkyRL

Discussion with [Eric Tang](mailto:etang@anyscale.com):

* SkyRL currently uses ActorGroups / Mesh only for training.  
* Using verl style collective communications for trajectory generation is too inflexible  
* For SkyRL, we introduced a new interface “Generator”, which generates the trajectories – it is a very flexible interface that allows arbitrary user code for execution, see [https://github.com/NovaSky-AI/SkyRL/blob/main/skyrl-train/skyrl\_train/generators/base.py\#L24](https://github.com/NovaSky-AI/SkyRL/blob/main/skyrl-train/skyrl_train/generators/base.py#L24) – currently there is one implementation that uses SkyGym to generate the trajectories: [https://github.com/NovaSky-AI/SkyRL/blob/main/skyrl-train/skyrl\_train/generators/skyrl\_gym\_generator.py\#L407](https://github.com/NovaSky-AI/SkyRL/blob/main/skyrl-train/skyrl_train/generators/skyrl_gym_generator.py#L407) – it uses Python async and thread pools pretty heavily, scale up / multi-node story needs to be figured out  
* Generation is called here [https://github.com/NovaSky-AI/SkyRL/blob/main/skyrl-train/skyrl\_train/trainer.py\#L652](https://github.com/NovaSky-AI/SkyRL/blob/main/skyrl-train/skyrl_train/trainer.py#L652)  
* Weight synching is orchestrated from the trainer side [https://github.com/NovaSky-AI/SkyRL/blob/main/skyrl-train/skyrl\_train/weights\_manager.py\#L57](https://github.com/NovaSky-AI/SkyRL/blob/main/skyrl-train/skyrl_train/weights_manager.py#L57)

Also interesting: Verl sandbox integration [https://verl.readthedocs.io/en/latest/sglang\_multiturn/sandbox\_fusion.html](https://verl.readthedocs.io/en/latest/sglang_multiturn/sandbox_fusion.html)

If we adapt Option 1 above for the collective API, one very natural way to expose would be via an option to ray.method that indicates this method call should only be scheduled on a single actor.

Monarch has a `.call` and `.call_one` for the two different use cases.