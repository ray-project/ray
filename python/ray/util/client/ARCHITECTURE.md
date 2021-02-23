# Ray Client Architecture Guide

A quick development primer on the codebase layout

## General

The Ray client is a gRPC client and server. 
The server runs `ray.init()` and acts like a normal Ray driver, controlled by the gRPC connection.
It does all the bookkeeping and keeps things in scope for the clients that connect.
Generally, the client side lives in `ray/util/client` and the server lives in `ray/util/client/server`.
By convention, the `ray/util/client` avoids importing `ray` directly, but the server side, being just another Ray application, is allowed to do so.
This separation exists both for dependency cycle reasons and also to, if desired, pull either portion out into it's own repo or sub-installation.

The `ray` variable of type `RayAPIStub` in `ray/util/client/__init__.py` acts as the equivalent API surface as does the `ray` package. 
Functions in the `ray` namespace are methods on the RayAPIStub object.

For many of the objects in the root `ray` namespace, there is an equivalent client object, mostly contained in `ray/util/client/common.py`.
So things have a mirror, like
```
ObjectRef <-> ClientObjectRef
RemoteFunc <-> ClientRemoteFunc
```

If you've acquired a ClientObjectRef, it should have come from the client code (ie, constructed by returning from the server).
How the two interchange is talked about under Protocol.

## Protocol

There's one gRPC spec for the client, and that lives at `//src/ray/protobuf/ray_client.proto`. 

There's another protocol at play, however, and that is _the way in which functions and data are encoded_.
This is particularly important in the context of the Ray client; since both ends are Python, this is a `pickle`.

### gRPC services

The gRPC side is the most straightforward and easiest to follow from the proto file.

#### get, put, and function calls

Client started life as a set of Unary RPCs, with just enough functionality to implement the most-used APIs. 
As an introduction to the RPC API, they're a good place to start to understand how the protocol works.
The proto file is well-commented with every field describing what it does.

The Unary RPCs are still around, but they are ripe for deprecation. 
The problem they have is that they are not tied to a persistent connection.
If you imagine a load balancer in front of a couple client-servers, then any client could hit any state on any server with a unary RPC. 
As we need to keep handles in sync for connected clients, that means one RPC could go to one server (say, a `x = f.remote()`), and the follow up (`ray.get(x)`) wouldn't have the corresponding ObjectRef on the other server.

Get, Put, and Wait are pretty standard. 
The more interesting one is Schedule, which implies a Put before it (the function to execute) and then executes it.

#### Data Channel

Which brings us to the data channel. 
The data channel is a bidirectional streaming connection for the client. 
It wraps all the same Request/Reponse patterns as the Unary RPCs.
At the start, the client associates itself with the server with a UUID-generated ClientID. 
As long as the channel is open, the client is connected.
Tracking the ClientID then allows us to track all the resources we're holding for a particular client, and we know if the client has disconnected (the channel drops). 

It's also through this mechanism we can do reference counting. 
The client can keep track of how many references it has to various Ray client objects, and, if they fall out of scope, can send a `ReleaseRequest` to the server to optimistically clean up after itself.
Otherwise, all reference counting is done on the client side and the server only needs to know when to clean up.
The server can also clean up all references held for a client whenever it thinks it's safe to do so. 
In the future, having an explicit "ClientDisconnection" message may help here, to delineate between a client that's intentionally done and will never come back and one that's experiencing a connectivity issue.

#### Logs Channel

Similar to the data channel, there's an associated logs channel which will pipe logs back to the client.
It's a separate channel as it's ancillary to the continued connection of the client, and if some logs are dropped due to disconnection, that's generally okay. 
It's also then a separate way for a log aggregator to connect without implementing the full API.
This is also a bidirectional stream, where the client sends messages to control the verbosity and type of content, and the server streams back all the logs as they come in.

### On CloudPickle

## Integration points with Ray core

In order to provide a seamless client experience with Ray core, we need to wrap some of the core Ray functions (eg, `ray.get()`).
Python's dynamic nature helps us here. As mentioned, the `RayAPIStub` is a class, not a module, which is a subtle difference. 
This also allows us to have a `__getattr__` on the API level to redirect whereever we'd like, which we can't do in modules ([at least until Python 3.6 is deprecated](https://www.python.org/dev/peps/pep-0562/))
If the `ray` core were an object instead of functions in a namespace, we wouldn't need to wrap them to integrate, we'd simply swap the implementation. 
But we have backwards compatibility to maintain.

All the interesting integration points with Ray core live within `ray/_private/client_mode_hook.py`. 
In that file are contextmanagers and decorators meant to wrap Ray core functions. 
If `client_mode_should_convert()` returns `True`, based on the environment variables as they've been set, then the decorators spring into action, and forward the calls to the `ray/util/client` object. 

## Testing
