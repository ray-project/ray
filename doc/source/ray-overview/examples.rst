.. _ref-ray-examples:

Ray Example Gallery
===================

.. dropdown:: Click here to generate your custom ML example with using our wizard

    At the core of every ML project sits model training, for which you need to use a framework.
    Start by picking one now, and we'll guide you through the next steps:

    .. raw:: html

        <div id="wizardMain">
            <form id="wizardMainForm">

            <div id="trainSelection">
                <h3 class="text-justify">What training framework do you want to use?</h3>
                <div class="radio-button-group" role="group">
                    <div class="item">
                      <input class="radio-button" type="radio" name="trainGroup" id="pytorchTrain" value="torch">
                      <label for="pytorchTrain">PyTorch</label>
                    </div>
                    <div class="item">
                      <input class="radio-button" type="radio" name="trainGroup" id="tfTrain" value="tf">
                      <label for="tfTrain">TensorFlow</label>
                    </div>
                    <!-- <div class="item">
                      <input class="radio-button" type="radio" name="trainGroup" id="xgbTrain" value="xgboost">
                      <label for="xgbTrain">XGBoost</label>
                    </div> -->
                </div>
                <p id="trainDesc"></p>
            </div>

            <div id="dataSelection" style="display:none;">
                <h3 class="text-justify">How do you load your data?</h3>
                <div class="radio-button-group" role="group">
                    <div class="item">
                      <input class="radio-button" type="radio" name="dataGroup" id="nativeData" value="nativedata">
                      <label for="nativeData">I already have a way to load data, e.g. PyTorch Dataloader.</label>
                    </div>
                    <div class="item">
                      <input class="radio-button" type="radio" name="dataGroup" id="rayData" value="raydata">
                      <label for="rayData">I still need to implement data loading. Let's use Ray Data for it.</label>
                    </div>
                </div>
                <p id="dataDesc"></p>
            </div>

            <div id="dataTypeSelection" style="display:none;">
                <h3 class="text-justify">What kind of data are you using?</h3>
                <div class="radio-button-group" role="group">
                    <!-- <div class="item">
                      <input class="radio-button" type="radio" name="dataTypeGroup" id="anyData" value="any">
                      <label for="anyData">Any Data</label>
                    </div> -->
                    <div class="item">
                      <input class="radio-button" type="radio" name="dataTypeGroup" id="imageData" value="image">
                      <label for="imageData">Image Data</label>
                    </div>
                    <div class="item">
                      <input class="radio-button" type="radio" name="dataTypeGroup" id="textData" value="text">
                      <label for="textData">Text Data</label>
                    </div>
                </div>
                <p id="dataTypeDesc"></p>
            </div>

            <div>
                <h4 id="exampleSelectionDesc"></h4>
            </div>

            <div>
                <div id="generateButton" type="button" class="btn btn-primary" style="display:none;">
                    Generate Example
                </div>
            </div>

            <div>
                <div id="wizardCode">
                </div>
            </div>

            </form>
            <form id="wizardDoMoreForm">

            <div id="doMoreSelection" style="display:none;">
                <h3 class="text-justify">After training...</h3>
                <div class="radio-button-group" role="group">
                    <div class="item">
                      <input class="radio-button" type="radio" name="doMoreGroup" id="batchInference" value="batch_inference">
                      <label for="batchInference">Run offline batch inference</label>
                    </div>
                    <div class="item">
                      <input class="radio-button" type="radio" name="doMoreGroup" id="onlineServing" value="serve">
                      <label for="onlineServing">Serve model using Ray Serve</label>
                    </div>
                </div>
                <h4 id="doMoreDesc"></h4>
            </div>

            <div>
                <div id="doMoreCode">
                </div>
            </div>

            </form>
        </div>

.. raw:: html

    <div class="searchWrap">
       <div class="searchDiv">
          <input type="text" id="searchInput" class="searchTerm"
           placeholder="What are you looking for? (Ex.: PyTorch, Tune, RL)">
          <button id="filterButton" type="button" class="searchButton">
            <i class="fa fa-search"></i>
         </button>
       </div>
    </div>

.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto fixed-height-img

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://github.com/ray-project/ray-educational-materials/blob/main/Computer_vision_workloads/Semantic_segmentation/Scaling_batch_inference.ipynb
        :type: url
        :text: [Tutorial] Architectures for Scalable Batch Inference with Ray
        :classes: btn-link btn-block stretched-link scalableBatchInference
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.anyscale.com/blog/model-batch-inference-in-ray-actors-actorpool-and-datasets
        :type: url
        :text: [Blog] Batch Inference in Ray: Actors, ActorPool, and Datasets
        :classes: btn-link btn-block stretched-link batchActorPool
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /ray-core/examples/batch_prediction
        :type: ref
        :text: [Example] Batch Prediction using Ray Core
        :classes: btn-link btn-block stretched-link batchCore
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /data/examples/nyc_taxi_basic_processing
        :type: ref
        :text: [Example] Batch Inference on NYC taxi data using Ray Data
        :classes: btn-link btn-block stretched-link nycTaxiData

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /data/examples/ocr_example
        :type: ref
        :text: [Example] Batch OCR processing using Ray Data
        :classes: btn-link btn-block stretched-link batchOcr

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.anyscale.com/blog/training-one-million-machine-learning-models-in-record-time-with-ray
        :type: url
        :text: [Blog] Training One Million ML Models in Record Time with Ray
        :classes: btn-link btn-block stretched-link millionModels
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.anyscale.com/blog/many-models-batch-training-at-scale-with-ray-core
        :type: url
        :text: [Blog] Many Models Batch Training at Scale with Ray Core
        :classes: btn-link btn-block stretched-link manyModels
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /ray-core/examples/batch_training
        :type: ref
        :text: [Example] Batch Training with Ray Core
        :classes: btn-link btn-block stretched-link batchTrainingCore
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /data/examples/batch_training
        :type: ref
        :text: [Example] Batch Training with Ray Datasets
        :classes: btn-link btn-block stretched-link batchTrainingDatasets
    ---
    :img-top: /images/tune.png

    .. link-button:: /tune/tutorials/tune-run
        :type: ref
        :text: [Guide] Tune Basic Parallel Experiments
        :classes: btn-link btn-block stretched-link tuneBasicParallel
    ---
    :img-top: /images/tune.png

    .. link-button:: /ray-air/examples/batch_tuning
        :type: ref
        :text: [Example] Batch Training and Tuning using Ray Tune
        :classes: btn-link btn-block stretched-link tuneBatch
    ---
    :img-top: /images/carrot.png

    .. link-button:: https://www.youtube.com/watch?v=3t26ucTy0Rs
        :type: url
        :text: [Talk] Scaling Instacart fulfillment ML on Ray
        :classes: btn-link btn-block stretched-link instacartFulfillment

    ---
    :img-top: /images/serve.svg

    .. link-button:: https://www.youtube.com/watch?v=UtH-CMpmxvI
        :type: url
        :text: [Talk] Productionizing ML at Scale with Ray Serve
        :classes: btn-link btn-block stretched-link productionizingMLServe
    ---
    :img-top: /images/serve.svg

    .. link-button:: https://www.anyscale.com/blog/simplify-your-mlops-with-ray-and-ray-serve
        :type: url
        :text: [Blog] Simplify your MLOps with Ray & Ray Serve
        :classes: btn-link btn-block stretched-link simplifyMLOpsServe
    ---
    :img-top: /images/serve.svg

    .. link-button:: /serve/getting_started
        :type: ref
        :text: [Guide] Getting Started with Ray Serve
        :classes: btn-link btn-block stretched-link gettingStartedServe
    ---
    :img-top: /images/serve.svg

    .. link-button:: /serve/model_composition
        :type: ref
        :text: [Guide] Model Composition in Serve
        :classes: btn-link btn-block stretched-link compositionServe
    ---
    :img-top: /images/grid.png

    .. link-button:: /serve/tutorials/index
        :type: ref
        :text: [Gallery] Serve Examples Gallery
        :classes: btn-link btn-block stretched-link examplesServe
    ---
    :img-top: /images/grid.png

    .. link-button:: https://www.anyscale.com/blog?tag=ray_serve
        :type: url
        :text: [Gallery] More Serve Use Cases on the Blog
        :classes: btn-link btn-block stretched-link useCasesServe

    ---
    :img-top: /images/tune.png

    .. link-button:: /tune/getting-started
        :type: ref
        :text: [Guide] Getting Started with Ray Tune
        :classes: btn-link btn-block stretched-link gettingStartedTune
    ---
    :img-top: /images/tune.png

    .. link-button:: https://www.anyscale.com/blog/how-to-distribute-hyperparameter-tuning-using-ray-tune
        :type: url
        :text: [Blog] How to distribute hyperparameter tuning with Ray Tune
        :classes: btn-link btn-block stretched-link distributeHPOTune
    ---
    :img-top: /images/tune.png

    .. link-button:: https://www.youtube.com/watch?v=KgYZtlbFYXE
        :type: url
        :text: [Talk] Simple Distributed Hyperparameter Optimization
        :classes: btn-link btn-block stretched-link simpleDistributedHPO
    ---
    :img-top: /images/tune.png

    .. link-button:: https://www.anyscale.com/blog/hyperparameter-search-hugging-face-transformers-ray-tune
        :type: url
        :text: [Blog] Hyperparameter Search with 🤗 Transformers
        :classes: btn-link btn-block stretched-link HPOTransformers
    ---
    :img-top: /images/grid.png

    .. link-button:: /tune/examples/index
        :type: ref
        :text: [Gallery] Ray Tune Examples Gallery
        :classes: btn-link btn-block stretched-link examplesTune
    ---
    :img-top: /images/grid.png

    .. link-button:: https://www.anyscale.com/blog?tag=ray-tune
        :type: url
        :text: More Tune use cases on the Blog
        :classes: btn-link btn-block stretched-link useCasesTune

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.youtube.com/watch?v=e-A93QftCfc
        :type: url
        :text: [Talk] Ray Train, PyTorch, TorchX, and distributed deep learning
        :classes: btn-link btn-block stretched-link pyTorchTrain
    ---
    :img-top: /images/uber.png

    .. link-button:: https://www.uber.com/blog/elastic-xgboost-ray/
        :type: url
        :text: [Blog] Elastic Distributed Training with XGBoost on Ray
        :classes: btn-link btn-block stretched-link xgboostTrain
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /train/train
        :type: ref
        :text: [Guide] Getting Started with Ray Train
        :classes: btn-link btn-block stretched-link gettingStartedTrain
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /ray-air/examples/huggingface_text_classification
        :type: ref
        :text: [Example] Fine-tune a 🤗 Transformers model
        :classes: btn-link btn-block stretched-link trainingTransformers
    ---
    :img-top: /images/grid.png

    .. link-button:: /train/examples
        :type: ref
        :text: [Gallery] Ray Train Examples Gallery
        :classes: btn-link btn-block stretched-link examplesTrain
    ---
    :img-top: /images/grid.png

    .. link-button:: https://www.anyscale.com/blog?tag=ray_train
        :type: url
        :text: [Gallery] More Train Use Cases on the Blog
        :classes: btn-link btn-block stretched-link useCasesTrain

    ---
    :img-top: /rllib/images/rllib-logo.png

    .. link-button:: https://applied-rl-course.netlify.app/
        :type: url
        :text: [Course] Applied Reinforcement Learning with RLlib
        :classes: btn-link btn-block stretched-link appliedRLCourse
    ---
    :img-top: /rllib/images/rllib-logo.png

    .. link-button:: https://medium.com/distributed-computing-with-ray/intro-to-rllib-example-environments-3a113f532c70
        :type: url
        :text: [Blog] Intro to RLlib: Example Environments
        :classes: btn-link btn-block stretched-link introRLlib
    ---
    :img-top: /rllib/images/rllib-logo.png

    .. link-button:: /rllib/rllib-training
        :type: ref
        :text: [Guide] Getting Started with RLlib
        :classes: btn-link btn-block stretched-link gettingStartedRLlib
    ---
    :img-top: /images/riot.png

    .. link-button:: https://www.anyscale.com/events/2022/03/29/deep-reinforcement-learning-at-riot-games
        :type: url
        :text: [Talk] Deep reinforcement learning at Riot Games
        :classes: btn-link btn-block stretched-link riotRL
    ---
    :img-top: /images/grid.png

    .. link-button:: /rllib/rllib-examples
        :type: ref
        :text: [Gallery] RLlib Examples Gallery
        :classes: btn-link btn-block stretched-link examplesRL
    ---
    :img-top: /images/grid.png

    .. link-button:: https://www.anyscale.com/blog?tag=rllib
        :type: url
        :text: [Gallery] More RL Use Cases on the Blog
        :classes: btn-link btn-block stretched-link useCasesRL

    ---
    :img-top: /images/shopify.png

    .. link-button:: https://shopify.engineering/merlin-shopify-machine-learning-platform
        :type: url
        :text: [Blog] The Magic of Merlin - Shopify's New ML Platform
        :classes: btn-link btn-block stretched-link merlin
    ---
    :img-top: /images/uber.png

    .. link-button:: https://drive.google.com/file/d/1BS5lfXfuG5bnI8UM6FdUrR7CiSuWqdLn/view
        :type: url
        :text: [Slides] Large Scale Deep Learning Training and Tuning with Ray
        :classes: btn-link btn-block stretched-link uberScaleDL
    ---
    :img-top: /images/carrot.png

    .. link-button:: https://www.instacart.com/company/how-its-made/griffin-how-instacarts-ml-platform-tripled-ml-applications-in-a-year/
        :type: url
        :text: [Blog] Griffin: How Instacart’s ML Platform Tripled in a year
        :classes: btn-link btn-block stretched-link instacartMLPlatformTripled
    ---
    :img-top: /images/predibase.png

    .. link-button:: https://www.youtube.com/watch?v=B5v9B5VSI7Q
        :type: url
        :text: [Talk] Predibase - A low-code deep learning platform built for scale
        :classes: btn-link btn-block stretched-link predibase
    ---
    :img-top: /images/gke.png

    .. link-button:: https://cloud.google.com/blog/products/ai-machine-learning/build-a-ml-platform-with-kubeflow-and-ray-on-gke
        :type: url
        :text: [Blog] Building a ML Platform with Kubeflow and Ray on GKE
        :classes: btn-link btn-block stretched-link GKEMLPlatform
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.youtube.com/watch?v=_L0lsShbKaY
        :type: url
        :text: [Talk] Ray Summit Panel - ML Platform on Ray
        :classes: btn-link btn-block stretched-link summitMLPlatform

    ---
    :img-top: /images/text-classification.png

    .. link-button:: /ray-air/examples/huggingface_text_classification
        :type: ref
        :text: [Example] Text classification with Ray
        :classes: btn-link btn-block stretched-link trainingTransformers
    ---
    :img-top: /images/image-classification.webp

    .. link-button:: /ray-air/examples/torch_image_example
        :type: ref
        :text: [Example] Image classification with Ray
        :classes: btn-link btn-block stretched-link torchImageExample
    ---
    :img-top: /images/credit.png

    .. link-button:: /ray-air/examples/feast_example
        :type: ref
        :text: [Example] Credit scoring with Ray and Feast
        :classes: btn-link btn-block stretched-link feastExample
    ---
    :img-top: /images/tabular-data.png

    .. link-button:: /ray-air/examples/xgboost_example
        :type: ref
        :text: [Example] Machine learning on tabular data
        :classes: btn-link btn-block stretched-link xgboostExample
    ---
    :img-top: /images/timeseries.png

    .. link-button:: /ray-core/examples/automl_for_time_series
        :type: ref
        :text: [Example] AutoML for Time Series with Ray
        :classes: btn-link btn-block stretched-link timeSeriesAutoML
    ---
    :img-top: /images/grid.png

    .. link-button:: /ray-air/examples/index
        :type: ref
        :text: [Gallery] Full Ray AIR Examples Gallery
        :classes: btn-link btn-block stretched-link AIRExamples

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.businessinsider.com/openai-chatgpt-trained-on-anyscale-ray-generative-lifelike-ai-models-2022-12
        :type: url
        :text: [Blog] How OpenAI Uses Ray to Train Tools like ChatGPT
        :classes: btn-link btn-block stretched-link chatgpt
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.anyscale.com/blog/building-highly-available-and-scalable-online-applications-on-ray-at-ant
        :type: url
        :text: [Blog] Highly Available and Scalable Online Applications on Ray at Ant Group
        :classes: btn-link btn-block stretched-link antServing

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.anyscale.com/blog/ray-forward-2022
        :type: url
        :text: [Blog] Ray Forward 2022 Conference: Hyper-scale Ray Application Use Cases
        :classes: btn-link btn-block stretched-link rayForward

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://www.anyscale.com/blog/ray-breaks-the-usd1-tb-barrier-as-the-worlds-most-cost-efficient-sorting
        :type: url
        :text: [Blog] A new world record on the CloudSort benchmark using Ray
        :classes: btn-link btn-block stretched-link rayForward

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /ray-core/examples/web-crawler
        :type: ref
        :text: [Example] Speed up your web crawler by parallelizing it with Ray
        :classes: btn-link btn-block stretched-link webCrawler
