# Ray Data AI Pipelines Hands On

At a high level, we'll look at using Ray Data for...

* Scalable data ingestion
* Transforming data using Ray Data pipelines and operators
* Scalable batch inference processing with accelerators
* Joining Ray Datasets and apply data transformation to joined columns
* Integrating scalable LLM inference and fractional resource scheduling

## Setup and Dependencies

```bash
pip install \
  torch==2.6.0 \
  torchvision==0.21.0 \
  matplotlib==3.10.1 \
  diffusers==0.32.2 \
  transformers==4.50.0 \
  accelerate==1.5.2
```

```python
from diffusers import AutoPipelineForText2Image
from transformers import pipeline
from transformers.utils import logging
import numpy as np
import random
import ray
import torch

logging.set_verbosity_info()
```

## Batch image generation

### Scenario

We have a dataset of image prompts (in our example, animals) and another dataset with enhanced detail information for each record (in the demo, clothing the animal will wear).

Our end goal is to combine the prompts and details, then use a LLM to enhance to prompts further, employ an image gen model to create corresponding images, and produce batch output to storage.

### Ray Data motivation

It's pretty easy to write a Python script to manipulate strings and use models directly from Huggingface with code like the following:

```python
pipe = AutoPipelineForText2Image.from_pretrained("stabilityai/sdxl-turbo", torch_dtype=torch.float16, variant="fp16").to("cuda")

image = pipe("A cinematic shot of a racoon wearing an italian priest robe.", num_inference_steps=1, guidance_scale=0.0).images[0]
```
But we want to build a scalable data+AI processing pipeline. To do that, we want to ...

* leverage a scale-out cluster with multiple GPUs
* read data using as much of our cluster as is useful (parallel read)
* work with the data in chunks large enough to get benefits of scale (i.e., not suffer from excessive overhead relative to the number of records)
    * but also small enough to allow for flexible scheduling as it flows through our pipeline -- we don't want an enormous chunk to hold up processing, require excessive disk or network I/O, etc.
* assign work to, e.g., CPU nodes where GPU is not required; or to smaller, cheaper GPUs where large ones are not required
* adjust batching to optimize GPU use even when ideal batch size may be different for different operations
* handle arbitrarily large datasets by leveraging a streaming execution model
* minimize I/O costs by, e.g., fusing operations where possible
* produce predictable flow by managing backpressure (i.e., ensuring data doesn't "pile up" in between pipeline stages)
* optimize via lazy execution and flexible logical + physical planners

Ray Data is designed to address these requirements, allowing us to orchestrate at scale while still straightforward Python / Huggingface code we're used to.

### Agenda and steps for incremental implementation

1.  Locate our datasets in shared storage
2.  Read records using Ray Data and learn how to perform basic transformations
3.  Generate images across multiple GPU nodes
4.  Lab activity: generate images and store all of our prompts and outputs as parquet data
5.  Join animal records against clothing outfit details to build a bigger prompt and generate enhanced images
6.  Lab activity: generate and export just the images as PNG files
7.  Leverage a LLM to further enhance the prompts, adding seasonal content and generate images from the full pipeline
8.  Lab activity: parameterize the LLM-based component so see how Ray Data supports separation of concerns
9.  Wrapup

---
First, we need to get all of our data in some common location where the whole cluster can see it. This might be a blob store, NFS, database, etc.

Anyscale offers `/mnt/cluster_storage` as a NFS path.

```bash
cp *.csv /mnt/cluster_storage/
```

Ray Data's `read_xxxx` methods (see I/O in Ray Docs for all the available formats and data sources) get us scalable, parallel reads.

```python
animals = ray.data.read_csv('/mnt/cluster_storage/animals.csv')

animals.take_batch(3)
```

Batches of records are represented as Python dicts where the keys correspond to the dataset column names and the values are a vectorized type -- usually a NumPy Array -- of values containing one value for each record in the batch.

Ray Data contains methods for basic data transformation and allow modification of dataset schema.

```python
animals.rename_columns({'animal' : 'prompt'}).take_batch(3)
```

Stateful tranformation of datasets -- in this example, AI inference where the state is the image gen model -- is done with the following pattern.

1.  Define a Python class (which Ray will later instantiate across the cluster as one more actor instances to do the processing)
2.  Use Dataset's `map_batches` API to tell Ray to send batches of data to the `__call__` method in the actors instances
    1.  `map_batches` allows us to specify resource requirements, actor pool size, batch size, and more

```python
class ImageGen():
    def __init__(self):
        self.pipe = AutoPipelineForText2Image.from_pretrained("stabilityai/sdxl-turbo", torch_dtype=torch.float16, variant="fp16").to("cuda")
        
    def gen_image(self, prompts):
        return self.pipe(prompt=list(prompts), num_inference_steps=1, guidance_scale=0.0).images
    
    def __call__(self, batch):
        batch['image'] = self.gen_image(batch['prompt'])
        return batch
```

```python
animals_images = animals.repartition(2).rename_columns({'animal' : 'prompt'}).map_batches(ImageGen, num_gpus=1, concurrency=2, batch_size=8)
```

Ray Datasets employ **lazy evaluation** for improved performance, so we can use APIs like `take_batch`, `take`, or `show` to trigger execution for development and testing purposes.

```python
examples = animals_images.take_batch(3)

examples
```

```python
examples['image'][0]
```
---
## Lab: Generate and write all output to storage as parquet data

Instructions/hints:

1.  Start with the Ray Dataset you'd like to write
2.  Check https://docs.ray.io/en/latest/data/api/input_output.html to find a suitable write API
3.  Remember to write to a *shared* file location, such as `/mnt/cluster_storage`

```python
# try your code here
```

---
## Load and join details for each prompt

Ray Data supports a number of high-performance JOIN APIs: https://docs.ray.io/en/latest/data/joining-data.html

We can use a JOIN to connect our animal records with a detailed prompt refinement unique to that record

```python
outfits = ray.data.read_csv('/mnt/cluster_storage/outfits.csv')

outfits.take_batch(3)
```

```python
animals_outfits = animals.join(outfits, 'inner', 1).repartition(8)

animals_outfits.take_batch(3)
```

We can add custom logic to combine and expand the image gen prompt using another call to `map_batches`

In this pattern, since the transformation is stateless and lightweight, we can define it as a Python function (which takes and returns a batch of records) and then use a simplified call to `map_batches` where Ray will autoscale the number of scheduled tasks in order to keep the best throughput for our pipeline.

```python
def expand_prompt(batch):
    batch['prompt'] = batch['animal'] + ' wearing a ' + batch['outfit']
    return batch
```

```python
animals_outfits.map_batches(expand_prompt).take_batch(3)
```

We can combine the prompt expansion operation with the image gen operation to produce a new set of results

```python
dressed_animals = animals_outfits.map_batches(expand_prompt).map_batches(ImageGen, batch_size=16, concurrency=2, num_gpus=1)
```

```python
examples = dressed_animals.take_batch(3)
```

```python
examples
```

```python
examples['prompt'][0]
```

```python
examples['image'][0]
```
---
## Lab: generate images for the input prompts and write the images to a folder

> Hint 1: Use `dataset.write_images(...)`
>
> Hint 2: To use `dataset.write_images(...)`, the images will need to be NumPy arrays (instead of PIL Image objects). You can use `np.array(my_pil_image)` to do that conversion. Use that API along with `map_batches` to convert all of your images prior to calling `write_images`

```python
# try your code here
```

---
## Enhance pipeline with LLM-generation of prompts

We can leverage a LLM to create more varied and detailed image prompts -- as well as add dynamism like a seasonal element -- by adding a LLM batch inference step to the pipeline.

To implement this operation, we'll
1.  Create a Python class to encasulate the logic and data transformtion
2.  Use `map_batches` to route batches of data from our Ray Dataset through this transformation operation
3.  Demonstrate Ray's support for fractional resource allocation, so that we can schedule 4 GPU-dependent operator instances with only 2 GPUs
4.  Demonstrate the decoupling of operator batch sizes from each other (as well as from Dataset block size) to optimally use our models and GPUs

```python
class Enhancer():
    def __init__(self):
        self.pipe = pipeline("text-generation", model="Qwen/Qwen2.5-0.5B-Instruct", device='cuda')
        
    def chat(self, prompts):
        messages = []
        for p in prompts:
            season = random.choice(['winter', 'spring', 'summer', 'fall'])
                                    
            message = [{"role": "system", "content": "You are a helpful assistant." +
                                "Enhance the image description with two short elements corresponding to the " + season + 
                                "season. Keep animal wearing clothing and retain image medium information (like photo or painting). Return new description only, no intro."},
                                {"role": "user", "content": p }]
            messages.append(message)
        return [out[0]['generated_text'][-1]['content'] for out in self.pipe(messages, max_new_tokens=200, batch_size=2)]
    
    def __call__(self, batch):
        batch['prompt'] = self.chat(batch['prompt'])
        return batch
```

```python
seasonal_images = animals_outfits.map_batches(expand_prompt) \
                        .map_batches(Enhancer, batch_size=4, concurrency=2, num_gpus=0.6) \
                        .map_batches(ImageGen, batch_size=8, concurrency=2, num_gpus=0.4)
```

```python
examples = seasonal_images.take_batch(5)
```

```python
examples['prompt'][0]
```

```python
examples['image'][0]
```
---
## Lab: Modify the `Enhancer` class and `seasonal_images` pipeline for parametrization

* Use variables contaioning the model name and the name of the dataset column containing the prompt as below

```python
enhancer_model = "Qwen/Qwen2.5-0.5B-Instruct"
prompt_column = "prompt"
```

```python
# try your code here: updated Enhancer class
```

```python
# try your code here: updated pipelineto generate seasonal_images Ray dataset
```

```python
examples = seasonal_images.take_batch(4)
```

```python
examples['prompt'][0]
```

```python
examples['image'][0]
```
---
## Wrapup

* Q&A
* Next Steps
