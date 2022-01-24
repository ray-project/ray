![Ray logo](images/ray_header_logo.png)

# If you are building distributed applications
**[Ray Core](ray-core/index.md) provides a simple, universal API for building distributed applications.**

Ray accomplishes this mission by:

1. Providing simple primitives for building and running distributed applications.
2. Enabling end users to parallelize single machine code, with little to zero code changes.
3. Including a large ecosystem of applications, libraries, and tools on top of the core Ray to enable complex applications.


After installing Ray with `pip install ray`, here's how you start a local Ray cluster in two lines of code:

<div class="termy">

```console
$ import ray
$ ray.init()

2021-12-20 12:45:11,656	INFO services.py:1340 -- View the Ray dashboard at http://127.0.0.1:8265
{'node_ip_address': '127.0.0.1', 'raylet_ip_address': '127.0.0.1', 'redis_address': '127.0.0.1:6379', 'object_store_address': '/tmp/ray/session_2021-12-20_12-45-09_357883_95594/sockets/plasma_store', 'raylet_socket_name': '/tmp/ray/session_2021-12-20_12-45-09_357883_95594/sockets/raylet', 'webui_url': '127.0.0.1:8265', 'session_dir': '/tmp/ray/session_2021-12-20_12-45-09_357883_95594', 'metrics_export_port': 57266, 'node_id': '2be6ccc5db99d2748876762216c51cfa7c841e9a8d7c18ec07c7eb9c'}
```

</div>

**Ray Core** provides simple primitives for building distributed applications.

# If you are building machine learning solutions
On top of Ray Core are several libraries for solving problems in machine learning:

- [Ray Data (beta)](ray-ml/ray-data/index.md): distributed data loading and compute
- [Ray Train](ray-ml/ray-train/index.md): distributed deep learning
- [Ray Tune](ray-ml/ray-tune/index.md): scalable hyperparameter tuning
- [Ray RLlib](ray-ml/ray-rllib/index.md): industry-grade reinforcement learning

As well as libraries for taking ML and distributed apps to production:

- [Ray Serve](ray-ml/ray-serve/index.md): scalable and programmable serving
- [Ray Workflows (alpha)](ray-ml/ray-workflows/index.md): fast, durable application flows

There are also many [community integrations](ray-ecosystem/integrations/integrations.md) with Ray, including [Dask](https://docs.ray.io/en/latest/data/dask-on-ray.html), [MARS](https://docs.ray.io/en/latest/data/mars-on-ray.html), [Modin](https://github.com/modin-project/modin), [Horovod](https://horovod.readthedocs.io/en/stable/ray_include.html), [Hugging Face](https://huggingface.co/transformers/main_classes/trainer.html#transformers.Trainer.hyperparameter_search), [Scikit-learn](ray-ecosystem/integrations/joblib.md), and others. Check out the [full list of Ray distributed libraries here](ray-ecosystem/integrations/integrations.md).


# If you are deploying Ray on your infrastructure
TODO

# Getting Involved

Ray is more than a framework for distributed applications but also an active community of developers,
researchers, and folks that love machine learning. Here's a list of tips for getting involved with the Ray community:

- Join our [community Slack](https://forms.gle/9TSdDYUgxYs8SA9e8) to discuss Ray!
- Star and follow us on [GitHub](https://github.com/ray-project/ray).
- To post questions or feature requests, check out the [Discussion Board](https://discuss.ray.io/).
- Follow us and spread the word on [Twitter](https://twitter.com/raydistributed).
- Join our [Meetup Group](https://www.meetup.com/Bay-Area-Ray-Meetup/) to connect with others in the community.
- Use the `[ray]` tag on [StackOverflow](https://stackoverflow.com/questions/tagged/ray) to ask and answer questions about Ray usage.

If you're interested in contributing to Ray, visit our page on [Getting Involved](ray-contributor-guide/getting-involved.md) to read about the contribution process and see what you can work on!

=== "Python"

    ``` python
    # First, run `pip install ray`.

    import ray
    ray.init()

    @ray.remote
    def f(x):
        return x * x

    futures = [f.remote(i) for i in range(4)]
    print(ray.get(futures)) # [0, 1, 4, 9]

    @ray.remote
    class Counter(object):
        def __init__(self):
            self.n = 0

        def increment(self):
            self.n += 1

        def read(self):
            return self.n

    counters = [Counter.remote() for i in range(4)]
    [c.increment.remote() for c in counters]
    futures = [c.read.remote() for c in counters]
    print(ray.get(futures)) # [1, 1, 1, 1]
    ```

=== "Java"

    First, add the `ray-api <https://mvnrepository.com/artifact/io.ray/ray-api>`__ and `ray-runtime <https://mvnrepository.com/artifact/io.ray/ray-runtime>`__ dependencies in your project.

    ```java
    import io.ray.api.ActorHandle;
    import io.ray.api.ObjectRef;
    import io.ray.api.Ray;
    import java.util.ArrayList;
    import java.util.List;
    import java.util.stream.Collectors;
    
    public class RayDemo {
    
      public static int square(int x) {
        return x * x;
      }
    
      public static class Counter {
    
        private int value = 0;
    
        public void increment() {
          this.value += 1;
        }
    
        public int read() {
          return this.value;
        }
      }
    
      public static void main(String[] args) {
        // Intialize Ray runtime.
        Ray.init();
        {
          List<ObjectRef<Integer>> objectRefList = new ArrayList<>();
          // Invoke the `square` method 4 times remotely as Ray tasks.
          // The tasks will run in parallel in the background.
          for (int i = 0; i < 4; i++) {
            objectRefList.add(Ray.task(RayDemo::square, i).remote());
          }
          // Get the actual results of the tasks with `get`.
          System.out.println(Ray.get(objectRefList));  // [0, 1, 4, 9]
        }
    
        {
          List<ActorHandle<Counter>> counters = new ArrayList<>();
          // Create 4 actors from the `Counter` class.
          // They will run in remote worker processes.
          for (int i = 0; i < 4; i++) {
            counters.add(Ray.actor(Counter::new).remote());
          }
    
          // Invoke the `increment` method on each actor.
          // This will send an actor task to each remote actor.
          for (ActorHandle<Counter> counter : counters) {
            counter.task(Counter::increment).remote();
          }
          // Invoke the `read` method on each actor, and print the results.
          List<ObjectRef<Integer>> objectRefList = counters.stream()
              .map(counter -> counter.task(Counter::read).remote())
              .collect(Collectors.toList());
          System.out.println(Ray.get(objectRefList));  // [1, 1, 1, 1]
        }
      }
    }
    ```

=== "C++"

    | The C++ Ray API is currently experimental with limited support. You can track its development `here <https://github.com/ray-project/ray/milestone/17>`__ and report issues on GitHub.
    | Run the following commands to get started:

    | - Install ray with C++ API support and generate a bazel project with the ray command.

    ``` shell
      pip install "ray[cpp]"
      mkdir ray-template && ray cpp --generate-bazel-project-template-to ray-template
    ```

    | - The project template comes with a simple example application. You can try this example out in 2 ways:
    | - 1. Run the example application directly, which will start a Ray cluster locally.

    ``` shell
      cd ray-template && bash run.sh
    ```

    | - 2. Connect the example application to an existing Ray cluster by specifying the RAY_ADDRESS env var.

    ``` shell
      ray start --head
      RAY_ADDRESS=127.0.0.1:6379 bash run.sh
    ```

    | - Now you can build your own Ray C++ application based on this project template.