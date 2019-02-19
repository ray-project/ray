# Ray Serve Module

`ray.serve` is a module for publishing your actors to interact with outside world. 

## Use Case

### Serve machine learning model


### Scalable anayltics query


### Composible pipelines


## Architecture

`ray.serve` is implemented in a three-tiered system. Each tier can scale horizontally. 

In the following illustration, call chain goes from top to bottom. 
Each box is one or more replicated ray actors.

```
             +-------------------+     +-----------------+   +------------+
Frontend     |   HTTP Frontend   |     |    Arrow RPC    |   |    ...     |
  Tier       |                   |     |                 |   |            |
             +-------------------+     +-----------------+   +------------+

             +------------------------------------------------------------+

                  +--------------------+        +-------------------+
 Router           |   Default Router   |        |   Deadline Aware  |
  Tier            |                    |        |      Router       |
                  +--------------------+        +-------------------+

             +------------------------------------------------------------+

                 +----------------+   +--------------+    +-------------+
 Managed         |  Managed Actor |   |     ...      |    |     ...     |
 Actor           |    Replica     |   |              |    |             |
 Tier            +----------------+   +--------------+    +-------------+
```

### Frontend Tier
The frontend tier is repsonsible for interface with the world. Currently `ray.serve` provides
implementation for 
- HTTP Frontend

And we are planning to add support for 
- Arrow RPC
- zeromq

### Router Tier
The router tier receives calls from frontend and route them to the managed actors. Routers both _route_
and _queue_ incoming queries. `ray.serve` has native support for (micro-)batching queries. 

In addition, we implemented a deadline aware routers that will put high priority queries in the front
of the queue so they will be delivered first. 

### Managed Actor Tier
Managed actors will be managed by routers. These actors can contains arbitrary methods. Methods in the 
actors class are assumed to be able to take into a batch of input at a time. If this cannot be assumed,
you can use the `@single_input` decorator, it will run your method in a for loop working on the micro-batch. 