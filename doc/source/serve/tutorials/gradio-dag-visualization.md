(serve-gradio-dag-visualization)=
# Visualizing a Deployment Graph with Gradio

You can visualize the [deployment graph](serve-model-composition-deployment-graph) you built with [Gradio](https://gradio.app/). This integration allows you to interactively run your deployment graph through the Gradio UI and see the intermediate outputs of each node in real time as they finish evaluation.

To access this feature, you need to install Gradio. 
:::{note} 
Gradio requires Python 3.7+. Make sure to install Python 3.7+ to use this tool.
:::
```console
pip install gradio
```

Additionally, you can optionally install `pydot` and `graphviz`. This will allow this tool to incorporate the complementary [graphical illustration](pydot-visualize-dag) of the nodes and edges.

::::{tab-set}

:::{tab-item} MacOS

```
pip install -U pydot && brew install graphviz
```

:::

:::{tab-item} Windows

```
pip install -U pydot && winget install graphviz
```

:::

:::{tab-item} Linux

```
pip install -U pydot && sudo apt-get install -y graphviz
```

:::

::::

Also, for the [quickstart example](gradio-vis-quickstart), install the `transformers` module to pull models through [HuggingFace's Pipelines](https://huggingface.co/docs/transformers/main_classes/pipelines).
```console
pip install transformers
```

(gradio-vis-quickstart)=
## Quickstart Example

Let's build and visualize a deployment graph that
  1. Downloads an image
  2. Classifies the image
  3. Translates the results to German.

This will be the graphical structure of our deployment graph:

![graph structure](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/gradio_visualization/graph_illustration.png)

Open up a new file named `demo.py`. First, let's take care of imports:
```{literalinclude} ../doc_code/gradio_dag_visualize.py
:start-after: __doc_import_begin__
:end-before: __doc_import_end__
:language: python
```

### Defining Nodes

The `downloader` function takes an image's URL, downloads it, and returns the image in the form of an `ImageFile`.
```{literalinclude} ../doc_code/gradio_dag_visualize.py
:start-after: __doc_downloader_begin__
:end-before: __doc_downloader_end__
:language: python
```

The `ImageClassifier` class, upon initialization, loads the `google/vit-base-patch16-224` image classification model using the Transformers pipeline. Its `classify` method takes in an `ImageFile`, runs the model on it, and outputs the classification labels and scores.
```{literalinclude} ../doc_code/gradio_dag_visualize.py
:start-after: __doc_classifier_begin__
:end-before: __doc_classifier_end__
:language: python
```

The `Translator` class, upon initialization, loads the `t5-small` translation model that translates from English to German. Its `translate` method takes in a map from strings to floats, and translates each of its string keys to German.
```{literalinclude} ../doc_code/gradio_dag_visualize.py
:start-after: __doc_translator_begin__
:end-before: __doc_translator_end__
:language: python
```

### Building the Graph

Finally, we can build our graph by defining dependencies between nodes.
```{literalinclude} ../doc_code/gradio_dag_visualize.py
:start-after: __doc_build_graph_begin__
:end-before: __doc_build_graph_end__
:language: python
```

### Deploy and Execute

Let's deploy and run the deployment graph! Deploy the graph with `serve run` and turn on the visualization with the `gradio` flag:
```console
serve run demo:serve_entrypoint --gradio
```

If you go to `http://localhost:7860`, you can now access the Gradio visualization! Type in a link to an image, click "Run", and you can see all of the intermediate outputs of your graph, including the final output!
![gradio vis](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/gradio_visualization/bear_example.png)

## Setting Up the Visualization
Now let's see how to set up this visualization tool.

### Requirement: Driver

The `DAGDriver` is required for the visualization. If the `DAGDriver` is not already part of your deployment graph, you can include it with:
```python
new_root_node = DAGDriver.bind(old_root_node)
```

### Ensure Output Data is Properly Displayed

Since the Gradio UI is set at deploy time, the type of Gradio component used to display intermediate outputs of the graph is also statically determined from the graph deployed. It is important that the correct Gradio component is used for each graph node.

The developer simply needs to specify the return type annotation of each function or method in the deployment graph.
:::{note}
If no return type annotation is specified for a node, then the Gradio component for that node will default to a [Gradio Textbox](https://gradio.app/docs/#textbox).
:::

The following table lists the supported data types and which Gradio component they're displayed on.

:::{list-table}
:widths: 60 40
:header-rows: 1
* - Data Type
  - Gradio component
* - `int`, `float`
  - [Numeric field](https://gradio.app/docs/#number)
* - `str`
  - [Textbox](https://gradio.app/docs/#textbox)
* - `bool`
  - [Checkbox](https://gradio.app/docs/#checkbox)
* - `pd.Dataframe`
  - [DataFrame](https://gradio.app/docs/#dataframe)
* - `list`, `dict`, `np.ndarray`
  - [JSON field](https://gradio.app/docs/#json)
* - `PIL.Image`, `torch.Tensor`
  - [Image](https://gradio.app/docs/#image)
:::

For instance, the output of the following function node will be displayed through a [Gradio Checkbox](https://gradio.app/docs/#textbox).
```python
@serve.deployment
def is_valid(begin, end) -> bool:
    return begin <= end
```

![Gradio checkbox example](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/gradio_visualization/checkbox_example.png)


### Providing Input

Similarly, the Gradio component used for each graph input should also be correct. For instance, a deployment graph for image classification could either take an image URL from which it downloads the image, or take the image directly as input. In the first case, the Gradio UI should allow users to input the URL through a textbox, but in the second case, the Gradio UI should allow users to upload the image through an Image component.

The data type of each user input can be specified by passing in `input_type` to `InputNode()`. The following two sections will describe the two supported ways to provide input through `input_type`.

The following table describes the supported input data types and which Gradio component is used to collect that data.

:::{list-table}
:widths: 60 40
:header-rows: 1
* - Data Type
  - Gradio component
* - `int`, `float`
  - [Numeric field](https://gradio.app/docs/#number)
* - `str`
  - [Textbox](https://gradio.app/docs/#textbox)
* - `bool`
  - [Checkbox](https://gradio.app/docs/#checkbox)
* - `pd.Dataframe`
  - [DataFrame](https://gradio.app/docs/#dataframe)
* - `PIL.Image`, `torch.Tensor`
  - [Image](https://gradio.app/docs/#image)
:::

#### Single Input

If there is a single input to the deployment graph, it can be provided directly through `InputNode`. The following is an example code snippet.

```python
with InputNode(input_type=ImageFile) as user_input:
    f_node = f.bind(user_input)
```

:::{note}
Notice there is a single input, which is stored in `user_input` (an instance of `InputNode`). The data type of this single input must be one of the supported input data types.
:::
When initializating `InputNode()`, the data type can be specified by passing in a `type` variable to the parameter `input_type`. Here, the type is specified to be `ImageFile`, so the Gradio visualization will take in user input through an [Image component](https://gradio.app/docs/#image).
![single input example](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/gradio_visualization/single_input.png)

#### Multiple Inputs

If there are multiple inputs to the deployment graph, it can be provided by accessing attributes of `InputNode`. The following is an example code snippet.

```python
with InputNode(input_type={0: int, 1: str, "id": str}) as user_input:
    f_node = f.bind(user_input[0])
    g_node = g.bind(user_input[1], user_input["id"])
```

:::{note}
Notice there are multiple inputs: `user_input[0]`, `user_input[1]`, and `user_input["id"]`. They are accessed by indexing into `user_input`. The data types for each of these inputs must be one of the supported input data types.
:::

When initializing `InputNode()`, these data types can be specified by passing in a dictionary that maps key to `type` (where key is integer or string) to the parameter `input_type`. Here, the input types are specified to be `int`, `str`, and `str`, so the Gradio visualization will take in the three inputs through one [Numeric Field](https://gradio.app/docs/#number) and two [Textboxes](https://gradio.app/docs/#textbox).

![multiple input example](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/gradio_visualization/multiple_inputs.png)