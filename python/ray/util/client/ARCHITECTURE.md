# Ray Client Architecture Guide

A quick development primer on the codebase layout

## General

The Ray client is a gRPC client and server. 
The server runs `ray.init()` and acts like a normal Ray driver, controlled by the gRPC connection.
It does all the bookkeeping and keeps things in scope for the clients that connect.
Generally, the client side lives in `ray/util/client` and the server lives in `ray/util/client/server`.
By convention, the `ray/util/client` avoids importing `ray` directly, but the server side, being just another Ray application, is allowed to do so.
This separation exists both for dependency cycle reasons and also to, if desired in the future, pull either portion out into its own repo or sub-installation.
(eg, `pip install ray_client`)

The `ray` global variable of type `RayAPIStub` in [`ray/util/client/__init__.py`](./__init__.py) acts as the equivalent API surface as does the `ray` package. 
Functions in the `ray` namespace are methods on the RayAPIStub object.

For many of the objects in the root `ray` namespace, there is an equivalent client object. These are mostly contained in [`ray/util/client/common.py`](./common.py).
These objects are client stand-ins for their server-side objects. For example:
```
ObjectRef <-> ClientObjectRef
RemoteFunc <-> ClientRemoteFunc
```

This means that, if the type of the object you're looking at (say, in a bug report) is a ClientObjectRef, it should have come from the client code (ie, constructed by returning from the server).

How the two interchange is talked about under Protocol.

## Protocol

There's one gRPC spec for the client, and that lives at `//src/ray/protobuf/ray_client.proto`. 

There's another protocol at play, however, and that is _the way in which functions and data are encoded_. 
This is particularly important in the context of the Ray client; since both ends are Python, this is a `pickle`.

The key separation to understand is that `pickle` is how client objects, including the client-side stubs, get serialized and transported, opaquely, to the server.
The gRPC service is the API surface to implement remote, thin, client functionality.
The `pickle` protocol is how the data is encoded for that API.

### gRPC services

The gRPC side is the most straightforward and easiest to follow from the proto file.

#### get, put, and function calls

Client started life as a set of unary RPCs, with just enough functionality to implement the most-used APIs. 
As an introduction to the RPC API, they're a good place to start to understand how the protocol works.
The proto file is well-commented with every field describing what it does.

The Unary RPCs are still around, but they are ripe for deprecation. 
The problem they have is that they are not tied to a persistent connection.

If you imagine a load balancer in front of a couple client-servers, then any client could hit any state on any server with a unary RPC. 
As we need to keep handles to ray ObjectRefs and similar so that they don't go out of scope and dropped, these must stay in sync for connected clients. 
With unary RPCs, that means one RPC could go to one server (say, a `x = f.remote()`), and the follow up (`ray.get(x)`) wouldn't have the corresponding ObjectRef on the other server.

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

#### On CloudPickle

As per the introduction to this section, `pickle` and `cloudpickle` are the way to encode to Python-executable data for the generic transport of gRPC.

Ray Client provides its own pickle/unpickle subclasses in `client_pickler.py` and `server/server_pickler.py`. 
The reason it has its own subclasses is to solve the problem of mixing `Client*` stub objects.

This is easier to describe by example.
Suppose a `RemoteFunc`, `f()` calls another `RemoteFunc`, `g()`
`f()` has to have a reference to `g()`, and knows it's a `RemoteFunc` (calls `g.remote()`, say) and so the function `f()` gets serialized with pickle, and in that serialization data is an object of class `RemoteFunc` to be deserialized on the worker side.

In Ray client, `f()` and `g()` are `ClientRemoteFunc`s and work the same way.
In early versions of the Ray Client, a `ClientRemoteFunc` had to know whether it was on the server or client side, and either run like a normal `RemoteFunc` (on the server) or a client call on the client. 
This led to some interesting bugs where passing, or especially returning, client-side stub objects around. 
(Imagine if `f()` calls `g()` which builds and returns a new closure `h()`)

To simplify all of this, the pickler subclasses were written. 

Now, whenever a Client-stub-object is serialized, a struct (as of writing, a tuple) is stored in its place, and when deserialized, the server "fills in" the appropriate non-stub object. 
And vice versa -- if the server is encoding a return/response that is an `ObjectRef`, a tuple is passed on the wire instead and the deserializer on the client side turns it back into a `ClientObjectRef`

This means that the client side deals as much as possible in its stub objects, and the server side never sees a stub object, and there is a clean separation between the two. Now, a `ClientObjectRef` existing on the server is an error case, and not a case to be handled specially.
It also means that the server side works just like normal Ray and deals in the normal Ray objects and can encode them transparently into client objects as they are sent.
Because never the twain shall meet, it's much easier to model and debug what's going on.

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

There are two primary ways to test client code.

The first is to approach it from the context of knowing that we're testing a client/server app. 
We can run both ends of the connection, call the client side (that we know to be the client side), and see the effect on the server and vice-versa.
The set of tests of the form `test_client*.py` take this approach.

The other way to approach them is as a fixture where we're testing the API of Ray, with Ray's own tests, _as though it were normal Ray_. 
It's a highly powerful pattern, in that a test passing there means that the user can feel confident that things work the way they always have, client or not.
It's also a more difficult pattern to implement, as hooking the setup and shutdown of a client/server pair and a single-node ray instance are different, especially when these tests may make assumptions about how the fixtures were set up in the first place. 
It is, however, the only way to test the integration points. 
So generally speaking, if it's implementing a feature that makes the client work, it probably should be a test in the `test_client` series where one controls both ends and tests the client code.
If it's fixing a user-side API bug or an integration with Ray core, it's probably adapting or including a pre-existing unit test as part of Ray Client.
