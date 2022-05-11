(serve-faq)=

# Ray Serve FAQ

This page answers some common questions about Ray Serve. If you have more
questions, feel free to ask them in the [Discussion Board](https://discuss.ray.io/).

```{contents}
```

## How do I deploy Ray Serve?

See {doc}`deployment` for information about how to deploy Serve.

## How fast is Ray Serve?

We are continuously benchmarking Ray Serve. We can confidently say:

- Ray Serve's **latency** overhead is single digit milliseconds, often times just 1-2 milliseconds.
- For **throughput**, Serve achieves about 3-4k qps on a single machine.
- It is **horizontally scalable** so you can add more machines to increase the overall throughput.

You can checkout our [microbenchmark instruction](https://github.com/ray-project/ray/tree/master/python/ray/serve/benchmarks)
to benchmark on your hardware.

## Can I use `asyncio` along with Ray Serve?

Yes! You can make your servable methods `async def` and Serve will run them
concurrently inside a Python asyncio event loop.

## Are there any other similar frameworks?

Yes and no. We truly believe Serve is unique as it gives you end to end control
over the API while delivering scalability and high performance. To achieve
something like what Serve offers, you often need to glue together multiple
frameworks like Tensorflow Serving, SageMaker, or even roll your own
batching server.

## How does Serve compare to TFServing, TorchServe, ONNXRuntime, and others?

Ray Serve is *framework agnostic*, you can use any Python framework and libraries.
We believe data scientists are not bounded a particular machine learning framework.
They use the best tool available for the job.

Compared to these framework specific solution, Ray Serve doesn't perform any optimizations
to make your ML model run faster. However, you can still optimize the models yourself
and run them in Ray Serve: for example, you can run a model compiled by
[PyTorch JIT](https://pytorch.org/docs/stable/jit.html).

## How does Serve compare to AWS SageMaker, Azure ML, Google AI Platform?

Ray Serve brings the scalability and parallelism of these hosted offering to
your own infrastructure. You can use our [cluster launcher](cluster-cloud)
to deploy Ray Serve to all major public clouds, K8s, as well as on bare-metal, on-premise machines.

Compared to these offerings, Ray Serve lacks a unified user interface and functionality
let you manage the lifecycle of the models, visualize it's performance, etc. Ray
Serve focuses on just model serving and provides the primitives for you to
build your own ML platform on top.

## How does Serve compare to Seldon, KFServing, Cortex?

You can develop Ray Serve on your laptop, deploy it on a dev box, and scale it out
to multiple machines or K8s cluster without changing one lines of code. It's a lot
easier to get started with when you don't need to provision and manage K8s cluster.
When it's time to deploy, you can use Ray [cluster launcher](cluster-cloud)
to transparently put your Ray Serve application in K8s.

Compare to these frameworks letting you deploy ML models on K8s, Ray Serve lacks
the ability to declaratively configure your ML application via YAML files. In
Ray Serve, you configure everything by Python code.

## Is Ray Serve only for ML models?

Nope! Ray Serve can be used to build any type of Python microservices
application. You can also use the full power of Ray within your Ray Serve
programs, so it's easy to run parallel computations within your deployments.
