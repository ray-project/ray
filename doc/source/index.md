```{include} /_includes/overview/announcement.md
```

```{raw} html

<link rel="stylesheet"
      href="//cdnjs.cloudflare.com/ajax/libs/highlight.js/11.7.0/styles/atom-one-dark.min.css">
<script src="//cdnjs.cloudflare.com/ajax/libs/highlight.js/11.7.0/highlight.min.js"></script>
<script>hljs.highlightAll();</script>


<script>
    const toc = document.getElementsByClassName("bd-toc")[0];
    toc.style.cssText = 'display: none !important';
    const main = document.getElementById("main-content");
    main.style.cssText = 'max-width: 100% !important'; 
</script>

<title>Welcome to Ray!</title>


<div class="container">
  <div class="row">
    <div class="col-6">
      <h1>Welcome to Ray!</h1>
      <p>Ray is an open-source unified framework for scaling AI and Python applications. 
      It provides the compute layer for parallel processing so that 
      you don’t need to be a distributed systems expert.
      </p>
      <div class="image-header">
        <a href="https://github.com/ray-project/ray">
            <img src="/_static/img/github-fill.png" width="25px" height="25px" />
        </a>
        <a href="https://docs.google.com/forms/d/e/1FAIpQLSfAcoiLCHOguOm8e7Jnn-JJdZaCxPGjgVCvFijHB5PLaQLeig/viewform">
            <img src="/_static/img/slack-fill.png" width="25px" height="25px" />
        </a>
        <a href="https://twitter.com/raydistributed">
            <img src="/_static/img/twitter-fill.png" width="25px" height="25px" />
        </a>
      </div>
    </div>
    <div class="col-6">
      <iframe width="450px" height="240px" style="border-radius:"10px";"
       src="https://www.youtube.com/embed/Iu_F_EzQb5o" 
       title="YouTube Preview" 
       frameborder="0" allow="accelerometer; autoplay;" 
       allowfullscreen></iframe>
    </div>
  </div>
</div>

<div class="container">
    <h2>Getting Started</h2>
<div class="container">

<div class="container">
  <div class="row">
    <div class="col-4 info-box">
        <div class="image-header">
            <img src="/_static/img/ray_logo.png" width="30px" height="30px" />
            <h3>Learn basics</h3>
        </div>
        <p>Understand how the Ray framework scales your ML workflows.</p>      
        <a class="bold-link" href="./ray-overview/index.html" >Learn more ></a>      
    </div>
    <div class="col-4 info-box">
        <div class="image-header">
            <img src="/_static/img/download.png" width="30px" height="30px" />
            <h3>Install Ray</h3>
        </div>
        <p><pre><code class="nohighlight" style="margin:10px;">pip install "ray[default]"</code></pre></p>      
        <a class="bold-link" href="./ray-overview/installation.html" >Installation guide ></a>      
    </div>
    <div class="col-4 info-box">
        <div class="image-header">
            <img src="/_static/img/code.png" width="30px" height="30px" />
            <h3>Try it out</h3>
        </div>
        <p>Experiment with Ray with an introductory notebook.</p>
        <a class="bold-link" href="https://colab.research.google.com/github/maxpumperla/learning_ray/blob/main/notebooks/ch_02_ray_core.ipynb" 
        >Open the notebook></a>      
    </div>
  </div>
</div>


<div class="container">

<h2>Scaling with Ray</h2>

<div class="row">
    <div class="col-4">
        <div class="nav flex-column nav-pills" id="v-pills-tab" role="tablist" aria-orientation="vertical">
          <a class="nav-link active" id="v-pills-data-tab" data-toggle="pill" href="#v-pills-data" role="tab" aria-controls="v-pills-data" aria-selected="true">
            Distributed Data Ingest
          </a>
          <a class="nav-link" id="v-pills-training-tab" data-toggle="pill" href="#v-pills-training" role="tab" aria-controls="v-pills-training" aria-selected="false">
            Model Training
          </a>
          <a class="nav-link" id="v-pills-tuning-tab" data-toggle="pill" href="#v-pills-tuning" role="tab" aria-controls="v-pills-tuning" aria-selected="false">
            Hyperparameter Tuning
          </a>
          <a class="nav-link" id="v-pills-rl-tab" data-toggle="pill" href="#v-pills-rl" role="tab" aria-controls="v-pills-rl" aria-selected="false">
            Reinforcement Learning
          </a>
          <a class="nav-link" id="v-pills-serving-tab" data-toggle="pill" href="#v-pills-serving" role="tab" aria-controls="v-pills-serving" aria-selected="false">
            Model Serving
          </a>
          <a class="nav-link" id="v-pills-batch-tab" data-toggle="pill" href="#v-pills-batch" role="tab" aria-controls="v-pills-batch" aria-selected="false">
            Batch Inference
          </a>
        </div>
    </div>
    <div class="col-8">
        <div class="tab-content" id="v-pills-tabContent">
          <div class="tab-pane fade show active" id="v-pills-data" role="tabpanel" aria-labelledby="v-pills-data-tab">
            <pre style="margin:0"><code class="language-python">
from ray import data

# Load in some data.
dataset = data.read_csv("s3://bucket/path")

# Partition the dataset into shards.
dataset = dataset.repartition(1000)

# Preprocess and transform the data.
preprocessor = data.preprocessors.StandardScaler(["value"])
dataset_transformed = preprocessor.fit_transform(dataset)
            </code></pre>
              <div class="row">
                <div class="col-10">
                  <a href="#">Learn more | API references</a>
                </div>
                <div class="col-2">
                    <a href="#">
                        <img src="/_static/img/colab.png" height="25px" />
                    </a>
                </div>
              </div>
          </div>
          <div class="tab-pane fade" id="v-pills-training" role="tabpanel" aria-labelledby="v-pills-training-tab">
            <pre style="margin:0"><code class="language-python">
from ray.data import Dataset
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig

# Load and preprocess the data.
data.read_parquet("s3://bucket/path")
preprocessor = data.preprocessors.BatchMapper(map_fn)
def train_loop_per_worker():
    ...
    # Define custom training logic here.
    # Set up an integrated PyTorch Trainer.

trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    scaling_config=ScalingConfig(num_workers=100),
    preprocessor=preprocessor,
    dataset={"train": dataset},
)

# Run distributed training.
result = trainer.fit()
            </code></pre>
              <div class="row">
                <div class="col-10">
                  <a href="#">Learn more | API references</a>
                </div>
                <div class="col-2">
                    <a href="#">
                        <img src="/_static/img/colab.png" height="25px" />
                    </a>
                </div>
              </div>
          </div>
          <div class="tab-pane fade" id="v-pills-tuning" role="tabpanel" aria-labelledby="v-pills-tuning-tab">
            <pre style="margin:0"><code class="language-python">
from ray import tune
from ray.air.config import ScalingConfig
from ray.train.lightgbm import LightGBMTrainer

# Load in some data.
dataset = data.read_csv("s3://bucket/path")

# Create an integrated LightGBM Trainer.
trainer = LightGBMTrainer(
       label_column="y",
       params={"objective": "regression"},
       scaling_config=ScalingConfig(num_workers=100),
       datasets={"train": dataset},
)

# Create a Tuner with search space and scaling config.
tuner = tune.Tuner(
       trainer=trainer,
       param_space={
           "num_leaves": tune.randint(50, 150),
           "min_data_in_leaf": tune.uniform(500, 1000),
       },
       tune_config=tune.TuneConfig(num_samples=100,metric="loss", mode="min")
)

# Run distributed hyperparameter tuning.
result_grid = tuner.fit()
            </code></pre>
              <div class="row">
                <div class="col-10">
                  <a href="#">Learn more | API references</a>
                </div>
                <div class="col-2">
                    <a href="#">
                        <img src="/_static/img/colab.png" height="25px" />
                    </a>
                </div>
              </div>
          </div>
          <div class="tab-pane fade" id="v-pills-rl" role="tabpanel" aria-labelledby="v-pills-rl-tab">
            <pre style="margin:0"><code class="language-python">
from ray.rllib.algorithms.ppo import PPOConfig

# Configure the algorithm.
config = ( 
    PPOConfig()
    .environment("Taxi-v3")
    .rollouts(num_rollout_workers=2)
    .framework("tf2")
    .training(model={"fcnet_hiddens": [64, 64]})
    .evaluation(evaluation_num_workers=1)
)

# Build the algorithm.
algo = config.build()

# Run distributed training.
for _ in range(5):
    print(algo.train())

# Evaluate.
algo.evaluate()
            </code></pre>
              <div class="row">
                <div class="col-10">
                  <a href="#">Learn more | API references</a>
                </div>
                <div class="col-2">
                    <a href="#">
                        <img src="/_static/img/colab.png" height="25px" />
                    </a>
                </div>
              </div>       
          </div>
          <div class="tab-pane fade" id="v-pills-serving" role="tabpanel" aria-labelledby="v-pills-serving-tab">
            <pre style="margin:0"><code class="language-python">
import requests
from ray import serve

# Define a Ray Serve deployment
@serve.deployment(route_prefix="/")
class MyModelDeployment:
     def __init__(self, msg: str):
        # Initialize model state: could be very large neural net weights.
        self._msg = msg

     def __call__(self, request):
        return {"result": self._msg}

# Deploy the model.
serve.run(MyModelDeployment.bind(msg="Hello world!"))

# Query the deployment and print the result.
print(requests.get("http://localhost:8000/").json())
            </code></pre>
              <div class="row">
                <div class="col-10">
                  <a href="#">Learn more | API references</a>
                </div>
                <div class="col-2">
                    <a href="#">
                        <img src="/_static/img/colab.png" height="25px" />
                    </a>
                </div>
              </div>          
          </div>          
          <div class="tab-pane fade" id="v-pills-batch" role="tabpanel" aria-labelledby="v-pills-batch-tab">
            <pre style="margin:0"><code class="language-python">
CODE TBD
            </code></pre>
              <div class="row">
                <div class="col-10">
                  <a href="#">Learn more | API references</a>
                </div>
                <div class="col-2">
                    <a href="#">
                        <img src="/_static/img/colab.png" height="25px" />
                    </a>
                </div>
              </div>
          </div>
        </div>
    </div>
</div>
  
</div>

<div class="container">
    <h2>Beyond the basics</h2>
</div>

<div class="container">
  <div class="row">
    <div class="col-6 info-box">
        <div class="image-header">
            <img src="/_static/img/AIR.png" width="30px" height="30px" />
            <h3>Ray AI Runtime</h3>
        </div>
        <p>Scale the entire ML pipeline from data ingest to model serving with high-level Python APIs that integrate with popular ecosystem frameworks.</p>      
        <a class="bold-link" href="./ray-air/getting-started.html" >Learn more about AIR></a>      
    </div>
    <div class="col-6 info-box">
        <div class="image-header">
            <img src="/_static/img/Core.png" width="30px" height="30px" />
            <h3>Ray Core</h3>
        </div>
        <p>Scale generic Python code with simple, foundational primitives that enable a high degree of control for building distributed applications or custom platforms.</p>
        <a class="bold-link" href="./ray-overview/installation.html" >Learn more about Core ></a>      
    </div>
  </div>
</div>



<div class="container">
    <h2>Getting involved</h2>
<div class="container">


<div class="container">
  
  <div class="row">
    <div class="col-3 mx-2 mb-2">
        <h4>Join the community</h4> 
    </div>
    <div class="col-3 mx-2 mb-2">
        <h4>Get support</h4>
    </div>
    <div class="col-3 mx-2 mb-2">
        <h4>Contribute to Ray</h4>
    </div>
  </div>
  
  <div class="row">
    <div class="col-3 mx-2 mb-2 community-box">
        <div class="image-header">
            <img src="/_static/img/meetup.png" width="20px" height="20px" />
            <p>Attend community events</p>
        </div>    
    </div>
    <div class="col-3 mx-2 mb-2 community-box">
        <div class="image-header">
            <img src="/_static/img/slack-fill.png" width="20px" height="20px" />
            <p>Find community on Slack</p>
        </div>    
    </div>
    <div class="col-3 mx-2 mb-2 community-box">
        <a href="./" class="image-header">
            <img src="/_static/img/mail.png" width="20px" height="20px" />
            <p>Contributor guide</p>
        </a>    
    </div>
  </div>

  <div class="row">
    <div class="col-3 mx-2 mb-2 community-box">
        <div class="image-header">
            <img src="/_static/img/mail.png" width="20px" height="20px" />
            <p>Subscribe to the newsletter</p>
        </div>    
    </div>
    <div class="col-3 mx-2 mb-2 community-box">
        <div class="image-header">
            <img src="/_static/img/chat.png" width="20px" height="20px" />
            <p>Ask questions to the forum</p>
        </div>    
    </div>
    <div class="col-3 mx-2 mb-2 community-box">
        <a href="./" class="image-header">
            <img src="/_static/img/github-fill.png" width="20px" height="20px" />
            <p>Create a pull request</p>
        </a>    
    </div>
  </div>

 <div class="row">
    <div class="col-3 mx-2 mb-2 community-box">
        <div class="image-header">
            <img src="/_static/img/twitter-fill.png" width="20px" height="20px" />
            <p>Follow us on Twitter</p>
        </div>    
    </div>
    <div class="col-3 mx-2 mb-2 community-box">
        <div class="image-header">
            <img src="/_static/img/github-fill.png" width="20px" height="20px" />
            <p>Open an issue</p>
        </div>    
    </div>
  </div>

</div>


```
