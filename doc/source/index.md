```{include} /_includes/overview/announcement.md
```

```{title} Welcome to Ray!
```

```{raw} html

<link rel="stylesheet"
      href="//cdnjs.cloudflare.com/ajax/libs/highlight.js/11.7.0/styles/atom-one-dark.min.css">
</link>
<script src="//cdnjs.cloudflare.com/ajax/libs/highlight.js/11.7.0/highlight.min.js"></script>
<script>hljs.highlightAll();</script>


<script>
    const toc = document.getElementsByClassName("bd-toc")[0];
    toc.style.cssText = 'display: none !important';
    const main = document.getElementById("main-content");
    main.style.cssText = 'max-width: 100% !important'; 
</script>

<title>Welcome to Ray!</title>

<div style = "background-image: url(_static/img/LandingPageBG.JPG); background-repeat:no-repeat; background-size:cover;">
<div class="container remove-mobile">
  <div class="row">
    <div class="col-6">
      <h1 style="font-weight:600;">Welcome to Ray!</h1>
      <p>Ray is an open-source unified framework for scaling AI and Python applications. 
      It provides the compute layer for parallel processing so that 
      you don’t need to be a distributed systems expert.
      </p>
      <div class="image-header" style="padding:0px;">
        <a href="https://github.com/ray-project/ray">
            <img class="icon-hover" src="_static/img/github-fill.png" width="25px" height="25px" />
        </a>
        <a href="https://docs.google.com/forms/d/e/1FAIpQLSfAcoiLCHOguOm8e7Jnn-JJdZaCxPGjgVCvFijHB5PLaQLeig/viewform">
            <img class="icon-hover" src="_static/img/slack-fill.png" width="25px" height="25px" />
        </a>
        <a href="https://twitter.com/raydistributed">
            <img class="icon-hover" src="_static/img/twitter-fill.png" width="25px" height="25px" />
        </a>
      </div>
    </div>
    <div class="col-6">
      <iframe width="450px" height="240px" style="border-radius:"10px";"
       src="https://www.youtube.com/embed/Iu_F_EzQb5o?modestbranding=1" 
       title="YouTube Preview" 
       frameborder="0" allow="accelerometer; autoplay;" 
       allowfullscreen></iframe>
    </div>
  </div>
</div>
</div>


<div class="container" style="margin-bottom:30px; margin-top:80px; padding:0px;">
    <h2 style="font-weight:600;">Getting Started</h2>
    
<div class="grid-container">
  <a class="no-underline" href="./ray-overview/index.html" target="_blank"> <div class="info-box">
        <div class="image-header" style="padding:0px;">
            <img src="_static/img/ray_logo.png" width="44px" height="44px" />
            <h3 style="font-size:20px;">Learn basics</h3>
        </div>
        <p style="color:#515151;">Understand how the Ray framework scales your ML workflows.</p>      
        <p style="font-weight:600;">Learn more > </p>  
  </div> </a>  
   <div class="info-box">
        <div class="image-header" style="padding:0px;">
            <img src="_static/img/download.png" width="44px" height="44px" />
            <h3 style="font-size:20px;">Install Ray</h3>
        </div>
        <p><pre style="border:none; margin:0px;"><code class="nohighlight" style="margin:10px;">pip install "ray[default]"</code></pre></p>      
        <a class="no-underline" href="./ray-overview/installation.html" target="_blank"> <p style="font-weight:600; margin-bottom: 0px;">Installation guide ></p></a>
  </div>
  <a class="no-underline" href="https://colab.research.google.com/github/ray-project/ray-educational-materials/blob/main/Introductory_modules/Quickstart_with_Ray_AIR_Colab.ipynb"  target="_blank" 
        ><div class="info-box">
        <div class="image-header" style="padding:0px;">
            <img src="_static/img/code.png" width="44px" height="44px" />
            <h3 style="font-size:20px;">Try it out</h3>
        </div>
        <p style="color:#515151;">Experiment with Ray with an introductory notebook.</p>
        <p style="font-weight:600;">Open the notebook></p> 
  </div></a>
</div>
  
<div class="container remove-mobile" style="margin-bottom:30px; margin-top:80px; padding:0px;">


<h2 style="font-weight:600;">Scaling with Ray</h2>

<div class="row">
    <div class="col-4">
        <div class="nav flex-column nav-pills" id="v-pills-tab" role="tablist" aria-orientation="vertical" style="border-bottom:none;">
          <a class="nav-link active" id="v-pills-data-tab" data-toggle="pill" href="#v-pills-data" role="tab" aria-controls="v-pills-data" aria-selected="true" style="color:black; font-weigth: 500; margin-top:8px;">
            Distributed Data Ingest
          </a>
          <a class="nav-link" id="v-pills-training-tab" data-toggle="pill" href="#v-pills-training" role="tab" aria-controls="v-pills-training" aria-selected="false" style="color:black; font-weigth: 500; margin-top:8px;">
            Model Training
          </a>
          <a class="nav-link" id="v-pills-tuning-tab" data-toggle="pill" href="#v-pills-tuning" role="tab" aria-controls="v-pills-tuning" aria-selected="false" style="color:black; font-weigth: 500; margin-top:8px;">
            Hyperparameter Tuning
          </a>
          <a class="nav-link" id="v-pills-batch-tab" data-toggle="pill" href="#v-pills-batch" role="tab" aria-controls="v-pills-batch" aria-selected="false" style="color:black; font-weigth: 500; margin-top:12px;">
            Batch Inference
          </a>
          <a class="nav-link" id="v-pills-serving-tab" data-toggle="pill" href="#v-pills-serving" role="tab" aria-controls="v-pills-serving" aria-selected="false" style="color:black; font-weigth: 500; margin-top:8px;">
            Model Serving
          </a>
          <a class="nav-link" id="v-pills-rl-tab" data-toggle="pill" href="#v-pills-rl" role="tab" aria-controls="v-pills-rl" aria-selected="false" style="color:black; font-weigth: 500; margin-top:8px;">
            Reinforcement Learning
          </a>
        </div>
    </div>
    <div class="col-8">
        <div class="tab-content" id="v-pills-tabContent" style="box-shadow: 0px 6px 30px 5px rgba(3,28,74,0.12); border-radius:8px;">
          <div class="tab-pane fade show active" id="v-pills-data" role="tabpanel" aria-labelledby="v-pills-data-tab" style="user-select:none;">
            <pre style="margin:0;"><code class="language-python not-selectable">
from ray import data

# Step 1: read 100 files in parallel from S3 directory
dataset = data.read_csv(paths="s3://structured/data", parallelism=100)

# Step 2: partition the dataset into blocks
dataset = dataset.repartition(num_blocks=1000)

# Step 3: preprocess the data at scale, 1000 blocks in parallel
preprocessor = data.preprocessors.StandardScaler(columns=["value"])
dataset_transformed = preprocessor.fit_transform(dataset=dataset)

            </code></pre>
              <div class="row" style="padding:16px;">
                <div class="col-6">
                  <a href="./data/data.html" target="_blank">Learn more </a> | <a href="./data/api/api.html" target="_blank"> API references</a>
                </div>
                <div class="col-6" style="display: flex; justify-content: flex-end;">
                    <a href="https://colab.research.google.com/github/ray-project/ray/blob/master/doc/source/data/examples/nyc_taxi_basic_processing.ipynb" target="_blank" style="color:black;">
                        <img src="_static/img/colab.png" height="25px" /> Open in colab
                    </a>
                </div>
              </div>
          </div>
          <div class="tab-pane fade" id="v-pills-training" role="tabpanel" aria-labelledby="v-pills-training-tab" style="user-select:none;">
            <pre style="margin:0;"><code class="language-python not-selectable">
from ray.air.config import ScalingConfig
from ray.train.torch import TorchTrainer

# Step 1: setup PyTorch model training as you normally would
def train_loop_per_worker():
    model = ...
    train_dataset = ...
    for epoch in range(num_epochs):
        ...  # model training logic

# Step 2: setup Ray's PyTorch Trainer to run on 32 GPUs
trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    scaling_config=ScalingConfig(num_workers=32, use_gpu=True),
    datasets={"train": train_dataset},
)

# Step 3: run distributed model training on 32 GPUs
result = trainer.fit()
            </code></pre>
              <div class="row" style="padding:16px;">
                <div class="col-6">
                  <a href="./train/train.html" target="_blank">Learn more </a> | <a href="./train/api/api.html" target="_blank"> API references</a>
                </div>
                <div class="col-6" style="display: flex; justify-content: flex-end;">
                    <a href="https://colab.research.google.com/github/ray-project/ray-educational-materials/blob/main/Computer_vision_workloads/Semantic_segmentation/Scaling_model_training_colab.ipynb" style="color:black;" target="_blank">
                        <img src="_static/img/colab.png" height="25px" /> Open in colab
                    </a>
                </div>
              </div>
          </div>
          <div class="tab-pane fade" id="v-pills-tuning" role="tabpanel" aria-labelledby="v-pills-tuning-tab" style="user-select:none;" style="user-select:none;">
            <pre style="margin:0;"><code class="language-python not-selectable">
from ray import tune
from ray.air.config import ScalingConfig
from ray.train.lightgbm import LightGBMTrainer

train_dataset, eval_dataset = ...

# Step 1: setup Ray's LightGBM Trainer to train on 64 CPUs
trainer = LightGBMTrainer(
    ...
    scaling_config=ScalingConfig(num_workers=64),
    datasets={"train": train_dataset, "eval": eval_dataset},
)

# Step 2: setup Ray Tuner to run 1000 trials
tuner = tune.Tuner(
    trainer=trainer,
    param_space=hyper_param_space,
    tune_config=tune.TuneConfig(num_sa
    les=1000),
)

# Step 3: run distributed HPO with 1000 trials; each trial runs on 64 CPUs
result_grid = tuner.fit()

            </code></pre>
              <div class="row" style="padding:16px;">
                <div class="col-6">
                  <a href="./tune/index.html" target="_blank">Learn more </a> | <a href="./tune/api/api.html" target="_blank"> API references</a>
                </div>
                <div class="col-6" style="display: flex; justify-content: flex-end;">
                    <a href="https://github.com/ray-project/ray/blob/master/doc/source/tune/examples/lightgbm_example.ipynb" style="color:black;" target="_blank">
                        <img src="_static/img/github-fill.png" height="25px" /> Open in Github
                    </a>
                </div>
              </div>
          </div>
          <div class="tab-pane fade" id="v-pills-batch" role="tabpanel" aria-labelledby="v-pills-batch-tab" style="user-select:none;" style="user-select:none;">
            <pre style="margin:0;"><code class="language-python">
from ray.train.batch_predictor import BatchPredictor
from ray.train.torch import TorchPredictor

dataset = ...

# Step 1: create batch predictor to run inference at scale
batch_predictor = BatchPredictor.from_checkpoint(
    checkpoint=model_checkpoint, predictor_cls=TorchPredictor
)

# Step 2: run batch inference on 64 GPUs
results = batch_predictor.predict(dataset, batch_size=512, num_gpus_per_worker=64)
            </code></pre>
              <div class="row" style="padding:16px;">
                <div class="col-6">
                  <a href="./ray-core/examples/batch_prediction.html" target="_blank">Learn more </a>
                </div>
                <div class="col-6" style="display: flex; justify-content: flex-end;">
                    <a href="https://colab.research.google.com/github/ray-project/ray-educational-materials/blob/main/Computer_vision_workloads/Semantic_segmentation/Scaling_batch_inference_colab.ipynb" style="color:black;" target="_blank">
                        <img src="_static/img/colab.png" height="25px" /> Open in colab
                    </a>
                </div>
              </div>
          </div>
          <div class="tab-pane fade" id="v-pills-serving" role="tabpanel" aria-labelledby="v-pills-serving-tab" style="user-select:none;" style="user-select:none;">
            <pre style="margin:0;"><code class="language-python">
from ray import serve
from ray.serve import PredictorDeployment
from ray.train.lightgbm import LightGBMPredictor

# Deploy 50 replicas of the LightGBM model as a live endpoint.
# Convert incoming JSON requests into a DataFrame.
serve.run(
    PredictorDeployment.options(
        name="LightGBM_Service",
        num_replicas=50,
    ).bind(
        predictor_cls=LightGBMPredictor,
        checkpoint=lgbm_best_checkpoint,
        http_adapter=serve.http_adapters.pandas_read_json,
    )
)
            </code></pre>
              <div class="row" style="padding:16px;">
                <div class="col-6">
                   <a href="./serve/index.html" target="_blank">Learn more </a> | <a href="./serve/api/index.html" target="_blank"> API references</a>
                </div>
                <div class="col-6" style="display: flex; justify-content: flex-end;">
                    <a href="https://github.com/ray-project/ray/blob/master/doc/source/ray-air/examples/gptj_serving.ipynb" style="color:black;" target="_blank">
                        <img src="_static/img/github-fill.png" height="25px" /> Open in Github
                    </a>
                </div>
              </div>          
          </div>  
          <div class="tab-pane fade" id="v-pills-rl" role="tabpanel" aria-labelledby="v-pills-rl-tab" style="user-select:none;">
            <pre style="margin:0;"><code class="language-python not-selectable">
from ray.rllib.algorithms.ppo import PPOConfig

# Step 1: configure PPO to run 64 parallel workers to collect samples from the env.
ppo_config = (
    PPOConfig()
    .environment(env="Taxi-v3")
    .rollouts(num_rollout_workers=64)
    .framework("torch")
    .training(model=rnn_lage)
)

# Step 2: build the PPO algorithm
ppo_algo = ppo_config.build()

# Step 3: train and evaluate PPO
for _ in range(5):
    print(ppo_algo.train())

ppo_algo.evaluate()
            </code></pre>
              <div class="row" style="padding:16px;">
                <div class="col-6">
                  <a href="./rllib/index.html" target="_blank">Learn more </a> | <a href="./rllib/package_ref/index.html" target="_blank"> API references</a>
                </div>
                <div class="col-6" style="display: flex; justify-content: flex-end;">
                    <a href="https://github.com/anyscale/ray-summit-2022-training/blob/main/ray-rllib/ex_01_intro_gym_and_rllib.ipynb" style="color:black;" target="_blank">
                        <img src="_static/img/github-fill.png" height="25px" /> Open in Github
                    </a>
                </div>
              </div>       
          </div>
                  
          
        </div>
    </div>
</div>
  
</div>


<div class="container" style="margin-bottom:30px; margin-top:80px; padding:0px;">
    <h2 style="font-weight:600;">Beyond the basics</h2>
</div>

<div class = "grid-container">
  <div class="info-box-2">
        <div class="image-header" style="padding:0px;">
            <img src="_static/img/AIR.png" width="32px" height="32px" />
            <h3 style="font-size:20px; font-weight:600;">Ray AI Runtime</h3>
        </div>
        <p>Scale the entire ML pipeline from data ingest to model serving with high-level Python APIs that integrate with popular ecosystem frameworks.</p>      
        <a class="bold-link" style="letter-spacing:0.05em; text-transform:uppercase; font-weight:500;" href="./ray-air/getting-started.html" target="_blank">Learn more about AIR ></a>      
  </div>
  <div class="info-box-2">
        <div class="image-header" style="padding:0px;">
            <img src="_static/img/Core.png" width="32px" height="32px" />
            <h3 style="font-size:20px; font-weight:600;">Ray Core</h3>
        </div>
        <p>Scale generic Python code with simple, foundational primitives that enable a high degree of control for building distributed applications or custom platforms.</p>
        <a class="bold-link" style="letter-spacing:0.05em; text-transform:uppercase; font-weight:500;" href="./ray-core/walkthrough.html" target="_blank">Learn more about Core ></a>      
  </div>
  <div class="info-box-2">
        <div class="image-header" style="padding:0px;">
            <img src="_static/img/rayclusters.png" width="32px" height="32px" />
            <h3 style="font-size:20px; font-weight:600;">Ray Clusters</h3>
        </div>
        <p>Deploy a Ray cluster on AWS, GCP, Azure or kubernetes from a laptop to a large cluster to seamlessly scale workloads for production</p>      
        <a class="bold-link" style="letter-spacing:0.05em; text-transform:uppercase; font-weight:500;" href="./cluster/getting-started.html" target="_blank">Learn more about clusters ></a>      
  </div>
</div>


<div class="container" style="margin-bottom:5px; margin-top:80px; padding:0px;">
  <h2 style="font-weight:600;">Getting involved</h2>
</div>
  <div class="grid-container">
    <div> 
    <h4> Join the community </h4>
    <a class="no-underline" href="https://www.meetup.com/Bay-Area-Ray-Meetup/" target="_blank"> <div class="community-box">
        <div class="image-header">
            <img src="_static/img/meetup.png" width="24px" height="24px" />
            <p>Attend community events</p>
        </div>    
    </div></a>
    <a class="no-underline" href="https://share.hsforms.com/1Ee3Gh8c9TY69ZQib-yZJvgc7w85" target="_blank"> <div class="community-box">
        <div class="image-header">
            <img src="_static/img/mail.png" width="24px" height="24px" />
            <p>Subscribe to the newsletter</p>
        </div>  
    </div></a> 
    <a class="no-underline" href="https://twitter.com/raydistributed" target="_blank"> <div class="community-box">
        <div class="image-header">
            <img src="_static/img/twitter-fill.png" width="24px" height="24px" />
            <p>Follow us on Twitter</p>
        </div> 
    </div></a> 
  </div>
<div> 
    <h4> Get Support </h4>
     <a class="no-underline" href="https://docs.google.com/forms/d/e/1FAIpQLSfAcoiLCHOguOm8e7Jnn-JJdZaCxPGjgVCvFijHB5PLaQLeig/viewform" target="_blank"> <div class="community-box">
        <div class="image-header">
            <img src="_static/img/slack-fill.png" width="24px" height="24px" />
            <p>Find community on Slack</p>
        </div>     
    </div></a>
    <a class="no-underline" href="https://discuss.ray.io/" target="_blank"> <div class="community-box">
        <div class="image-header">
            <img src="_static/img/chat.png" width="24px" height="24px" />
            <p>Ask questions to the forum</p>
        </div>     
    </div></a>
    <a class="no-underline" href="https://github.com/ray-project/ray/issues/new/choose" target="_blank"> <div class="community-box">
        <div class="image-header">
            <img src="_static/img/github-fill.png" width="24px" height="24px" />
            <p>Open an issue</p>
        </div>  
    </div></a>
  </div>
  <div> 
    <h4> Contribute to Ray </h4>
    <a class="no-underline" href="./ray-contribute/getting-involved.html" target="_blank"> <div class="community-box">
        <div class="image-header">
            <img src="_static/img/mail.png" width="24px" height="24px" />
            <p>Contributor's guide</p>
        </div>     
    </div></a>
    <a class="no-underline" href="https://github.com/ray-project/ray/pulls" target="_blank"> <div class="community-box">
        <div class="image-header">
            <img src="_static/img/github-fill.png" width="24px" height="24px" />
            <p>Create a pull request</p>
        </div>     
    </div></a>
  </div>
</div>
```
