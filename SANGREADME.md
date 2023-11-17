Actor.bind would kill actors unless I cache the refs. We should fix it.
When actor calls are binded with actor.method.bind, it doesn't create a new DAG, but it append binded methods to existing DAG. 

Worker -> method1
       -> method 2

Instead of 2 dags with

method1
method 2

Only 1 input node is possible with current DAG API.

Serve: Got around the first issue because all actors are detached.
Not sure how it got around the second case. Maybe it never need to handle this case. 

Example:

worker = Worker.bind()
dag = worker.method.bind()
dag2 = worker.method_2.bind()

This will become

worker -> method -> method2

not 

worker -> method
worker -> method_2


VLLM

init_worker
init torch distributed
init_model
profile_num_available_blocks
init_cache_engine

forward

Q:
- How much existing DAG will be used? Are we going to implement our own DAG APIs? (I believe so?)
- What's the work needed to make .remote work with actors?
    - Is actor creation supposed to be a part of DAG?
- How the current shared memory based transport feature will be exposed to API?
- How do we handle different size input for different object ref? (the remaining bytes are just becoming garbages?)
- e2e flow
    - InputNode creates the first buffer (object_ref) that could be reused.
    - Each bind method reuses the buffer.
    - If actor is reused.
        - Use the first buffer created? We can only have 1 input node anyway now.
- Iterable DAG -> is it just a repeat of execute?

TODO
- [done] Curerntly, any bind from actor will become a huge single DAG starting from actor. 
    - Need to find a way to exclude ClassNode from DAG execution. 
- [done] Only one input node is possible for a single actor. But input node can have multiple inputs
    - Maybe we should allow multiple input node for a single actor (and use it as a starting point).
- [done] No way to keep the actor alive.
    - There's private argument _ray_cache_ref, but it will cache all refs which is not desirable.
    - New API in the part of bind.

1 DAG can only have 1 input Node
