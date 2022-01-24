# Ray Documentation

```{image} https://github.com/ray-project/ray/raw/master/doc/source/images/ray_header_logo.png
```

**Ray provides a simple, universal API for building distributed applications.**

Ray accomplishes this mission by:

1. Providing simple primitives for building and running distributed applications.
2. Enabling end users to parallelize single machine code, with little to zero code changes.
3. Including a large ecosystem of applications, libraries, and tools on top of the core Ray to enable complex applications.

**Ray Core** provides the simple primitives for application building.

On top of **Ray Core** are several libraries for solving problems in machine learning:

- {doc}`../tune/index`
- {ref}`rllib-index`
- {ref}`train-docs`
- {ref}`datasets` (beta)

As well as libraries for taking ML and distributed apps to production:

- {ref}`rayserve`
- {ref}`workflows` (alpha)

There are also many {ref}`community integrations <ray-oss-list>` with Ray, including [Dask], [MARS], [Modin], [Horovod], [Hugging Face], [Scikit-learn], and others. Check out the {ref}`full list of Ray distributed libraries here <ray-oss-list>`.

[dask]: https://docs.ray.io/en/latest/data/dask-on-ray.html
[horovod]: https://horovod.readthedocs.io/en/stable/ray_include.html
[hugging face]: https://huggingface.co/transformers/main_classes/trainer.html#transformers.Trainer.hyperparameter_search
[mars]: https://docs.ray.io/en/latest/data/mars-on-ray.html
[modin]: https://github.com/modin-project/modin
[scikit-learn]: joblib.html

```{admonition} Learn more and get involved
:class: tip full-width

💡 [Open an issue](https://github.com/executablebooks/jupyter-book/issues/new/choose)
: We track enhancement requests, bug-reports, and to-do items via GitHub issues.

💬 [Join the discussion](https://github.com/executablebooks/meta/discussions)
: We have community discussions, talk about ideas, and share general questions and feedback in our [community forum](https://github.com/executablebooks/meta/discussions).

👍 [Vote for new features](ebp:feature-note)
: The community provides feedback by adding a 👍 reaction to issues in our repositories.
You can find a list of the top issues [in the Executable Books issue leader board](ebp:feature-note).

🙌 [Join the community](contribute/intro.md)
: Jupyter Book is developed by the [Executable Books community](https://executablebooks.org).
We welcome anyone to join us in improving Jupyter Book and helping one another learn and create their books.
To join, check out our [contributing guide](contribute/intro.md).
```


## Find the right documentation resources

Here are a few pointers to help you get started.

````{panels}
:container: +full-width
:column: col-lg-4 px-2 py-2

---
:header: bg-info

<img src="ray-overview/images/ray_svg_logo.svg" alt="ray" width="50px">Data
^^^

Foo
+++
footer
---
:header: bg-info

<img src="ray-overview/images/ray_svg_logo.svg" alt="ray" width="50px">RLlib
^^^
Bar
+++
{badge}`example-badge,badge-primary`

---
:header: bg-info

<img src="ray-overview/images/ray_svg_logo.svg" alt="ray" width="50px">Train
^^^^^^^^^^^^^^^
Baz
+++
footer
````


## Getting Started with Ray

Check out {ref}`gentle-intro` to learn more about Ray and its ecosystem of libraries that enable
things like distributed hyperparameter tuning, reinforcement learning, and distributed training.

Ray provides Python, Java, and *EXPERIMENTAL* C++ API.
And Ray uses Tasks (functions) and Actors (classes) to allow you to parallelize your code.

```{eval-rst}
.. tabbed:: Python

    .. code-block:: python

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

```{eval-rst}
.. tabbed:: Java

    First, add the `ray-api <https://mvnrepository.com/artifact/io.ray/ray-api>`__ and `ray-runtime <https://mvnrepository.com/artifact/io.ray/ray-runtime>`__ dependencies in your project.

    .. code-block:: java

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

```{eval-rst}
.. tabbed:: C++

    | The C++ Ray API is currently experimental with limited support. You can track its development `here <https://github.com/ray-project/ray/milestone/17>`__ and report issues on GitHub.
    | Run the following commands to get started:

    | - Install ray with C++ API support and generate a bazel project with the ray command.

    .. code-block:: shell

      pip install "ray[cpp]"
      mkdir ray-template && ray cpp --generate-bazel-project-template-to ray-template

    | - The project template comes with a simple example application. You can try this example out in 2 ways:
    | - 1. Run the example application directly, which will start a Ray cluster locally.

    .. code-block:: shell

      cd ray-template && bash run.sh

    | - 2. Connect the example application to an existing Ray cluster by specifying the RAY_ADDRESS env var.

    .. code-block:: shell

      ray start --head
      RAY_ADDRESS=127.0.0.1:6379 bash run.sh

    | - Now you can build your own Ray C++ application based on this project template.
```

```{panels}
:body: bg-primary text-justify
:header: text-center
:footer: text-right

---
:column: + p-1

panel 1 header
^^^^^^^^^^^^^^

panel 1 content

++++++++++++++
panel 1 footer

---
:column: + p-1 text-center border-0
:body: bg-info
:header: bg-success
:footer: bg-secondary

panel 2 header
^^^^^^^^^^^^^^

panel 2 content

++++++++++++++
panel 2 footer
```

You can also get started by visiting our [Tutorials](https://github.com/ray-project/tutorial).
For the latest wheels (nightlies), see the [installation page](ray-overview/installation).

# Getting Involved

```{eval-rst}
.. include:: ray-contribute/involvement.rst
```

If you're interested in contributing to Ray, visit our page on {ref}`Getting Involved <getting-involved>`
to read about the contribution process and see what you can work on!


```{dropdown} My Content
:container: + shadow
:title: bg-primary text-white text-center font-weight-bold
:body: bg-light text-right font-italic
:animate: fade-in-slide-down

Is formatted
```

````{dropdown} Panels in a drop-down
:title: bg-success text-warning
:open:
:animate: fade-in-slide-down

```{panels}
:container: container-fluid pb-1
:column: col-lg-6 col-md-6 col-sm-12 col-xs-12 p-2
:card: shadow
:header: border-0
:footer: border-0

---
:card: + bg-warning

header
^^^^^^

Content of the top-left panel

++++++
footer

---
:card: + bg-info
:footer: + bg-danger

header
^^^^^^

Content of the top-right panel

++++++
footer

---
:column: col-lg-12 p-3
:card: + text-center

.. link-button:: panels/usage
    :type: ref
    :text: Clickable Panel
    :classes: btn-link stretched-link font-weight-bold
```
````

```{panels}
:body: bg-jb-two text-justify
:header: text-center
:footer: text-right

---
:column: + p-1

panel 1 header
^^^^^^^^^^^^^^

panel 1 content

++++++++++++++
panel 1 footer

---
:column: + p-1 text-center border-0
:body: bg-info
:header: bg-success
:footer: bg-secondary

panel 2 header
^^^^^^^^^^^^^^

panel 2 content

++++++++++++++
panel 2 footer
```