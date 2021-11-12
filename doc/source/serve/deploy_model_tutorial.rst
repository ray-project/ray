====================================
End-to-End Model Deployment Tutorial
====================================

By the end of this tutorial you will have learned how to deploy a machine learning model locally via Ray Serve.

First, install Ray Serve and all of its dependencies by running the following command in your terminal:

.. code-block:: python

  pip install "ray[serve]"

For this tutorial, we'll use `a model <https://huggingface.co/mrm8488/t5-base-finetuned-summarize-news>`_ that summarizes news articles. Let's take a look at how it works:

.. code-block:: python

  from transformers import AutoTokenizer, AutoModelWithLMHead
  
  def summarize(text, max_length=150):
    tokenizer = AutoTokenizer.from_pretrained("mrm8488/t5-base-finetuned-summarize-news")
    model = AutoModelWithLMHead.from_pretrained("mrm8488/t5-base-finetuned-summarize-news")

    input_ids = tokenizer.encode(text, return_tensors="pt", add_special_tokens=True)
    generated_ids = model.generate(input_ids=input_ids, num_beams=2, max_length=max_length,  repetition_penalty=2.5, length_penalty=1.0, early_stopping=True)
    preds = [tokenizer.decode(g, skip_special_tokens=True, clean_up_tokenization_spaces=True) for g in generated_ids]
    
    return preds[0]
  
  article_text = "HOUSTON -- Men have landed and walked on the moon. "        \
  "Two Americans, astronauts of Apollo 11, steered their fragile "            \
  "four-legged lunar module safely and smoothly to the historic landing "     \
  "yesterday at 4:17:40 P.M., Eastern daylight time. Neil A. Armstrong, the " \
  "38-year-old commander, radioed to earth and the mission control room "     \
  "here: \"Houston, Tranquility Base here. The Eagle has landed.\" The "      \
  "first men to reach the moon -- Armstrong and his co-pilot, Col. Edwin E. " \
  "Aldrin Jr. of the Air Force -- brought their ship to rest on a level, "    \
  "rock-strewn plain near the southwestern shore of the arid Sea of "         \
  "Tranquility. About six and a half hours later, Armstrong opened the "      \
  "landing craft\'s hatch, stepped slowly down the ladder and declared as "   \
  "he planted the first human footprint on the lunar crust: \"That\'s one "   \
  "small step for man, one giant leap for mankind.\" His first step on the "  \
  "moon came at 10:56:20 P.M., as a television camera outside the craft "     \
  "transmitted his every move to an awed and excited audience of hundreds "   \
  "of millions of people on earth."
  
  article_summary = summarize(article_text, 100)
  
  print(article_summary)

  '''
  Print output:

  and walked on the moon. The astronauts of Apollo 11 steered their craft to
  the historic landing yesterday at 4:17:40 P.M., Eastern daylight time.
  "That's one small step for man, one giant leap for mankind," Armstrong
  declared as he planted the first human footprint on the lunar crust. He
  radioed to earth and the mission control room: "Houston, Tranquility Base
  here. The Eagle has landed."
  '''

The ``tokenizer`` and ``model`` variables store our model's input encodings and weights. The ``summarize`` function takes in article ``text`` and the summary's ``max_length``, and then it summarizes the article text. In the example above, the ``summarize`` function generates a concise version of a lengthy news snippet about the moon landing.

The goal is to deploy this model using Ray Serve, so it can be queried over HTTP. We'll start by converting the above Python function into a Ray Serve deployment that can be run locally on a laptop.

First, we need to import ray, ray serve, and requests. The ray and ray serve libraries give us access to Ray Serve's deployments, so we can access our model over HTTP. The requests library handles HTTP request routing:

.. code-block:: python

  import ray
  from ray import serve
  import requests

After these imports, we can include our model code from above. We won't call our ``summarize`` function just yet though! We will soon add logic to handle HTTP requests, so the ``summarize`` function can operate on article text sent via HTTP request.

.. code-block:: python

  from transformers import AutoModelWithLMHead, AutoTokenizer

  def summarize(text, max_length=150):
    tokenizer = AutoTokenizer.from_pretrained("mrm8488/t5-base-finetuned-summarize-news")
    ...
    return preds[0]

Ray Serve needs to run on top of a Ray Cluster, so we start a local one:

.. code-block:: python

  ray.init()

.. note::

  ``ray.init()`` will start a single-node Ray cluster on your local machine, which will allow you to use all your CPU cores to serve requests in parallel.  To start a multi-node cluster, see :doc:`../cluster/index`.

Next, we start the Ray Serve runtime:

.. code-block:: python

  serve.start()

.. warning::

  When the Python script exits, Ray Serve will shut down.  
  If you would rather keep Ray Serve running in the background you can use ``serve.start(detached=True)`` (see :doc:`deployment` for details).

Now that we have defined our ``summarize`` function, launched a Ray Cluster, and started the Ray Serve runtime, we can define a function to accept HTTP requests and route them to the ``summarize`` function. Since ``summarize`` takes in article ``text`` and a summary ``max_length``, we need to take in these variables' values as parameters in the HTTP request URL. We can define function called ``router`` that takes in a Starlette ``request`` object. ``router`` then looks for the ``txt`` parameter in the Starlette ``request`` object to find the requested article text that must be summarized. It then passes in the article text, as well its length, into the ``summarize`` function and returns the value. We also add the decorator ``@serve.deployment`` to the ``router`` function to turn it into a Serve ``Deployment`` object.

.. code-block:: python

  @serve.deployment
  def router(request):
    txt = request.query_params["txt"]
    return summarize(txt, max_length=len(txt))

.. tip::
  This routing function's name doesn't have to be ``router``. It can be any function name as long as the corresponding name is present in the HTTP request. If you want the function name to be different than the name in the HTTP request, you can add the ``name`` keyword parameter to the ``@serve.deployment`` decorator to define the name sent in the HTTP request. In other words, if the decorator was ``@serve.deployment(name="responder")`` and the key signature was ``def request_manager(request)``, the HTTP request would use ``responder``, not ``request_manager``. If no ``name`` is passed into ``@serve.deployment``, the ``request`` will by default use the function's name instead. For example, if the decorator was ``@serve.deployment`` and the function's key signature was ``def manager(request)``, the HTTP request would use ``manager``.

Since ``@serve.deployment`` makes ``router`` a ``Deployment`` object, it can be deployed using ``router.deploy()``:

.. code-block:: python

  router.deploy()

Once we deploy ``router``, we can query the model over HTTP. And with that, we can run our model on Ray Serve! Here's the full Ray Serve deployment script that we built for our model:

.. code-block:: python
  import ray
  from ray import serve
  import requests
  from transformers import AutoModelWithLMHead, AutoTokenizer

  def summarize(text, max_length=150):
    tokenizer = AutoTokenizer.from_pretrained("mrm8488/t5-base-finetuned-summarize-news")
    ...
    return preds[0]

  ray.init()
  serve.start()

  @serve.deployment
  def router(request):
    txt = request.query_params["txt"]
    return summarize(txt, max_length=len(txt))

  router.deploy()

.. warning::

  When the Python script exits, Ray Serve will shut down.  
  If you would rather keep Ray Serve running in the background you can use ``serve.start(detached=True)`` (see :doc:`deployment` for details).

With our model deployed, we can test it over HTTP. The structure of our query will be:

``http://127.0.0.1:8000/[Deployment Name]?[Parameter Name-1]=[Parameter Value-1]&[Parameter Name-2]=[Parameter Value-2]&...&[Parameter Name-k]=[Parameter Value-k]``

Since the cluster is deployed locally in this tutorial, the ``127.0.0.1:8000`` refers to a localhost with port 8000. The ``[Deployment Name]`` refers to either the name of the function that we called ``.deploy()`` on (in our case, this is ``router``), or if the ``name`` keyword parameter in ``@serve.deployment`` was set, it's the value of that parameter (see the Tip under the ``router`` function definition above for more info). Each ``[Parameter Name]`` refers to the name of one of the fields in the request's ``query_params`` dictionary for our deployed function. In our example, the only parameter we need to pass in is ``txt``. This parameter is referenced in the ``txt = request.query_params["txt"]`` line in the ``router`` function. Each [Parameter Name] object has a corresponding [Parameter Value] object. The ``txt``'s [Parameter Value] will be a string containing the article text that we want to summarize. We can chain together any number of the name-value pairs using the ``&`` symbol in the request URL.

Now that the ``summarize`` function is deployed on Ray Serve, we can make http requests to it. Here's a client script that requests a summary from the same article as the original Python script:

.. code-block:: python
  import requests

  article_text = "HOUSTON -- Men have landed and walked on the moon. "        \
                                 ...
  "of millions of people on earth."
  response = requests.get("http://127.0.0.1:8000/router?txt=" + article_text).text
  print(response)

  '''
  Print output:

  and walked on the moon. The astronauts of Apollo 11 steered their craft to
  the historic landing yesterday at 4:17:40 P.M., Eastern daylight time.
  "That's one small step for man, one giant leap for mankind," Armstrong
  declared as he planted the first human footprint on the lunar crust. He
  radioed to earth and the mission control room: "Houston, Tranquility Base
  here. The Eagle has landed."
  '''

.. warning::

  Since Ray Serve shuts down when the Python deployment script finishes, we can either keep Ray Serve running in the background using ``serve.start(detached=True)`` (see :doc:`deployment` for details) or for testing purposes, we can add this client script to the end of the Serve deployment script and run it all together to see the output.

Our Serve deployment is still a bit inefficient though. In particular, the ``summarize`` function sets the input encodings and weights by defining the ``tokenizer`` and ``model`` variables each time that it's called. However, these encodings and weights never change, so it would be better if we could define ``tokenizer`` and ``model`` only once and keep their values in memory instead of reloading them upon each HTTP query.

We can achieve this by converting our ``summarize`` function into a class:

.. code-block:: python
    @serve.deployment
    class Summarizer:
        def __init__(self):
            self.tokenizer = AutoTokenizer.from_pretrained("mrm8488/t5-base-finetuned-summarize-news")
            self.model = AutoModelWithLMHead.from_pretrained("mrm8488/t5-base-finetuned-summarize-news")
        
        def __call__(self, request):
            txt = request.query_params["txt"]
            return self.summarize(txt, max_length=len(txt))

        def summarize(self, text, max_length=150):
            input_ids = self.tokenizer.encode(text, return_tensors="pt", add_special_tokens=True)
            generated_ids = self.model.generate(input_ids=input_ids, num_beams=2, max_length=max_length,  repetition_penalty=2.5, length_penalty=1.0, early_stopping=True)
            preds = [self.tokenizer.decode(g, skip_special_tokens=True, clean_up_tokenization_spaces=True) for g in generated_ids]
            
            return preds[0]
    
    Summarizer.deploy()

With this configuration, we can query the Summarizer class instead of a router function. When the ``Summarizer`` class is initialized, its ``__init__`` function is called, which loads and stores the input encodings and model weights in ``self.tokenizer`` and ``self.model``. HTTP queries for the ``Summarizer`` class will by default get routed to its ``__call__`` method, which takes in a Starlette ``request`` object. The ``Summarizer`` class can then take the request's ``txt`` article ``text`` data and call the ``summarize`` function on it. The ``summarize`` function no longer needs to load the input encodings and model weights on each query. Instead it can use the saved ``self.tokenizer`` and ``self.model`` values.

HTTP queries for the Ray Serve class deployments follow a similar format to Ray Serve function deployments. Here's an example client script for the ``Summarizer`` example:

.. code-block:: python
  import requests

  article_text = "HOUSTON -- Men have landed and walked on the moon. "        \
                                 ...
  "of millions of people on earth."
  response = requests.get("http://127.0.0.1:8000/Summarizer?txt=" + article_text).text
  print(response)

  '''
  Print output:

  and walked on the moon. The astronauts of Apollo 11 steered their craft to
  the historic landing yesterday at 4:17:40 P.M., Eastern daylight time.
  "That's one small step for man, one giant leap for mankind," Armstrong
  declared as he planted the first human footprint on the lunar crust. He
  radioed to earth and the mission control room: "Houston, Tranquility Base
  here. The Eagle has landed."
  '''

Congratulations! You just built and deployed a machine learning model on Ray Serve! You should now have enough context to dive into the :doc:`core-apis` to get a deeper understanding of Ray Serve.
To learn more about how to start a multi-node cluster for your Ray Serve deployments, see :doc:`../cluster/index`.
For more interesting example applications, including integrations with popular machine learning frameworks and Python web servers, be sure to check out :doc:`tutorials/index`.