# Ray plus FastAPI

API design ideas from [@tiangolo / Sebastián Ramírez](https://github.com/tiangolo) to integrate Ray with FastAPI.

Here are some personal and very opinionated ideas about integrating Ray with FastAPI and making it easier for FastAPI users to use Ray in their applications.

## Summary

It seems that currently Ray is designed to be an all encompassing framework that would cover all areas of development. This means that any code that would in some way touch or interact with Ray would be expected to be built with and deployed with Ray.

The current approach to integrate web/API development with Ray is with Ray Serve. Ray Serve expects users to use Ray patterns around classes and to deploy these web applications/APIs via Ray clusters and Ray mechanisms exclusively.

FastAPI doesn't directly support classes, and there's no clear way to develop standard FastAPI applications (based on functions) with Ray Serve while integrating it with the rest of Ray and Ray Serve.

The API design ideas shown here are mostly around allowing FastAPI developers to quickly and gradually adopt Ray or parts of it, without having to learn too much about everything that Ray offers, and without requiring them to change their mental model, deployments methods, or code structure.

## Decoupling Serving

Currently Ray Serve expects model serving and API serving to be tightly coupled, in the same Ray deployment classes.

The first idea is to decouple these two things. Ray Core, Ray Serve (or it could even be a new Ray component) could serve Python functions and classes directly, with Python objects and data types. The same way that the current Ray Serve `ServeHandle`s can be used inside of Ray Serve deployments to access other Ray deployment components, a similar mechanism could be available for non-Ray applications to access Ray deployments/objects.

And FastAPI applications could be deployed independently with other mechanisms, not exclusively with Ray, while being able to communicate with the components in Ray Serve through a Ray client from within the FastAPI app.

## Ray Serve client in FastAPI

Currently the only way (that I know of) of accessing a `ServeHandle` to be able to call it from somewhere else is with:

```Python
handle: RayServeSyncHandle = serve.run(Model.bind())
```

But that requires running `serve.run()` from the same environment that will access the handle.

I would like to be able to deploy Ray Serve model serving components that don't even need to expose a web API, but are accessed via Ray code APIs, and that could be accessed from other code.

The closest to this that I got was having a file for Ray Serve parts (model serving with pure Python objects), in a file, let's say called `batched.py` with something like this (from the examples in the docs):

```Python
from typing import List
import numpy as np
from ray import serve


@serve.deployment
class Model:
    @serve.batch(max_batch_size=8, batch_wait_timeout_s=0.1)
    async def __call__(self, multiple_samples: List[int]) -> List[int]:
        # Use numpy's vectorized computation to efficiently process a batch.
        return np.array(multiple_samples) * 2


handle = serve.run(Model.bind())
```

And then from a FastAPI app, let's say `app.py`, importing and using the `handle`:

```Python
from fastapi import FastAPI
import ray


from batched import handle

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/num/{num}")
async def read_spanish(num: int):
    value = await handle.remote(num)
    return int(value)
```

This looks very close to what I would like to have, but it doesn't work they way I would like it to work.

I would like to be able to have a FastAPI app deployed by myself in my own Docker container (or any other mechanism) and connect to Ray and its objects/deployments as a client, no deploying things from within the FastAPI app.

I would like the model serving parts (non-web-API Ray Serve) to be deployed independently, probably via Ray CLIs (e.g. `serve run batched:Model`).

And I would like FastAPI to be able to connect to and use that `Model` object (or `handle`) **without deploying it from the FastAPI app**.

Because currently the only way to acquire the `handle` is via `ray.run()` (as far as I know), getting that handle deploys/redeploys the Ray component (if I understand correctly).

I would like to be able to handle replication/scale for the FastAPI app on my own (e.g. with Kubernetes), and have each of the apps connect to Ray and access the pre-existing Ray Serve components via some other way to get the `handle` that doesn't involve redeploying it (it would be redeployed once per external container replicating the FastAPI app).

Being able to deploy the FastAPI app myself is important to keep the current existing code in already existing applications and systems that are not built from scratch with Ray. Or also in applications that for any other reason would not be deployed with Ray but with other mechanisms.

The first part of that would be that the FastAPI app would not start a Ray cluster directly, but instead connect to a pre-existing one.

It could initialize the Ray client in the `startup` event, or even at the top level of the module:

```Python
from fastapi import FastAPI
import ray


from batched import handle

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    ray.init(address="ray://127.0.0.1:10001")


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/num/{num}")
async def read_spanish(num: int):
    value = await handle.remote(num)
    return int(value)
```

This could probably be in the Ray docs.

...this still doesn't solve everything, as by importing the `handle` from `batched.py` it will still be deployed.

### Ray Serve taking a web port

The other part of this problem is that it's not possible to deploy Ray Serve components/deployments without them acquiring the web API port, even when they would be used only/mainly as an RPC mechanism to access and call Python objects (in my example).

### Imaginary code API

A possible imaginary Ray code API could look like this.

The `batched.py` file would not include the handle or deployment parts directly (or it would be under an `if __name__ == "__main__"` block):

```Python
from typing import List
import numpy as np
from ray import serve


@serve.deployment
class Model:
    @serve.batch(max_batch_size=8, batch_wait_timeout_s=0.1)
    async def __call__(self, multiple_samples: List[int]) -> List[int]:
        # Use numpy's vectorized computation to efficiently process a batch.
        return np.array(multiple_samples) * 2


if __name__ == "__main__":
    handle = serve.deploy(Model.bind())
```

It could be deployed by running it directly as a script or via a Ray CLI:

```console
$ serve deploy batched:Model
```

In this example I'm using the new "keyword" `deploy` to mean that it's not the same as Ray Serve `run`, this would not necessarily start a web server, but would start the `Model` deployment and have it available to handle remote calls.

**Note**: supporting calling the script directly with an `if __name__ == "__main__"` block or the CLI, or the use of `deploy` vs `run` would be just implementation details that could be adapted and changed as necessary. The main point is that it would be in some way an independent process to deploy a pure Python model object and a FastAPI app that interacts with it as a client.

Then on the FastAPI side, the app would connect to the Ray cluster, and it would be able to import the class, but by importing it that would not trigger a deploy. And there would be a mechanism to aquire a `handle` that would just connect to the already deployed model:

```Python
from fastapi import FastAPI
import ray


from batched import Model

app = FastAPI()
handle = ray.get_handle(Model)


@app.on_event("startup")
async def startup_event():
    ray.init(address="ray://127.0.0.1:10001")


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/num/{num}")
async def read_spanish(num: int):
    value = await handle.remote(num)
    return int(value)
```

The code looks very similar, but the `handle` is not created by calling `serve.run()` and triggering a deploy, but by acquiring a handle to the existing deployment.

**Note**: it's possible that everything that I'm suggesting is already possible. It's not obvious at least (after reading all the docs for Ray Serve). If there's already away to achieve this, I think that should probably be on one of the first tutorials about integrating FastAPI with Ray.

## Automatic Batch Processing

What I think probably is the **killer feature** of all this is the automatic batch processing.

It's already there, and by simplifying the examples it would be easier to highlight that.

What I imagine would be a great entrypoint for FastAPI developers to start adopting Ray would be to let them keep using their FastAPI applications and deployments. And then use Ray to deploy models with Ray Serve (or the equivalent component that would handle serving Python objects and calls, independent of web APIs).

Then from those standard FastAPI applications they would be able to connect to a Ray cluster and communicate with those deployments, calling inference for one data point, but having Ray transparently handle batch processing across the possibly multiple inference calls across different concurrent FastAPI requests.

## Batch Processing Typing

Something that could possibly be improved a bit is how the batch processing parts are typed.

Users should probably define some kind of function that takes a list of items of some type and returns a list of return values of some other type. Then they would transform that in some way with Ray, and they would receive something they could call that takes individual items and returns individual return values.

An example of how that could be annotated would be to ask users to provide a function that takes a list of inputs of some type and returns a list of outputs of some other type:

```Python
from typing import Callable, Generic, List, TypeVar, TypedDict

class Params(TypedDict):
    a: int
    b: float


def batch_process(data: List[Params]):
    return [param["a"] + param["b"] for param in data]
```

Then Ray would provide some way to transform that function and return something that would allow calling individual inference calls, this mock code would be provided by Ray (not written by users):

```Python
T1 = TypeVar("T1")
T2 = TypeVar("T2")


class Batcher(Generic[T1, T2]):
    def process(self, data: T1) -> T2: ...
    

def get_batcher(func: Callable[[List[T1]], List[T2]]) -> Batcher[T1, T2]:
    return Batcher()
```

Then users would be able to transform their batch processing function via Ray into something they can call in their code:

```Python
batcher = get_batcher(batch_process)

value = batcher.process(data={"a": 1, "b": 2.0})
```

If you copy these snippets of code, e.g. in VS Code, you will see that the editor correctly suggests (autocompletes) the dict parameters `"a"` and `"b"`, checks they have the correct types, and the return `value` has the correct type (`float`) too.

Something like this could then be used in FastAPI applications to communicate with Ray/Ray Serve and take advantage of batch processing easily.

And by providing some way of using this with improved types (e.g. with something as described above) FastAPI users would still be able to get the same type of editor and tooling support.

## FastAPI Dependencies

I was initially thinking that some of these parts could need or benefit from FastAPI dependencies using `Depends()`, but from all the examples and experiments I did, I didn't find some obvious benefit from them.

If it was not possible to have a global Ray client, or a global object handler, and it was necessary to have one per request, then that could benefit from dependencies. But as I didn't find anything that would certainly benefit from it I'm not including examples of that here as that would add unnecessary complexity to these ideas.

## FastAPI path operation deployments

🚨 **Warning**: overengineering alert.

Ray Serve and Ray in general have interesting ideas about deployments per Python objects instead of per files, per containers, etc.

If I understand correctly, when deploying a FastAPI app with Ray Serve, the replication would be done for the whole app. Or to get more granularity, it would be using Ray Serve classes.

I think the approach closest to FastAPI's design to allow more granularity would be defining deployments per *path operation* (routes, request handling functions). It would be something like this:

```Python
from fastapi import FastAPI
from ray import serve

app = FastAPI()


@serve.deployment(num_replicas=3)
@app.get("/")
def read_root():
    return {"Hello": "World"}


@serve.deployment(num_replicas=5)
@app.get("/num/{num}")
async def read_spanish(num: int):
    value = num + 3
    return int(value)
```

Nevertheless, I don't really think this would be too necessary and that users would need or benefit too much from this. I wouldn't expect each of the individual *path operations* to be much more expensive than the rest to justify divinding their work like that.

I would expect that the heavy lifting would be on the Ray deployments and objects. And those would be consumed from each one of these *path operations*, so the cost of these *path operations* themselves would be quite moderate and probably not worth the overengineering of allowing each one to have it's own deployment configurations.

## Documentation

It's also possible that some or many of these ideas are already possible. In that case, the action item would be just to document them very well.

The trick would probably be to document the simplest of use cases, with as much clarity as possible and as early in the documentation as possible, to guide users to follow some steps and achieve some win, get some value, with the minimum investment of time and effort possible.

## Conclusion

The general idea here is just to try to let FastAPI developers move as little as possible from their confort zone, to quickly adopt a little bit of Ray and have a win with that.

That will already be a benefit, and after that they will naturally want to explore more afterwards.
