.. _ray-oss-list:

The Ray Ecosystem
=================

This page lists libraries that have integrations with Ray for distributed execution
in alphabetical order.
It's easy to add your own integration to this list.
Simply open a pull request with a few lines of text, see the dropdown below for
more information.

.. dropdown:: Adding Your Integration

    To add an integration, simply add an entry to the `projects` list of our
    Gallery YAML on `GitHub <https://github.com/ray-project/ray/tree/master/doc/source/ray-overview/eco-gallery.yml>`_.

    .. code-block:: yaml

          - name: the integration link button text
            section_title: The section title for this integration
            description: A quick description of your library and its integration with Ray
            website: The URL of your website
            repo: The URL of your project on GitHub
            image: The URL of a logo of your project

    That's all!

.. grid:: 1 2 2 3
    :gutter: 1
    :class-container: container pb-3

    .. grid-item-card::

        .. figure:: ../images/buildflow.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/launchflow/buildflow?style=social)]
                :target: https://github.com/launchflow/buildflow

            BuildFlow is a backend framework that allows you to build and manage complex cloud infrastructure using pure python. With BuildFlow's decorator pattern you can turn any function into a component of your backend system.

        +++
        .. button-link:: https://docs.launchflow.com/buildflow/introduction
            :color: primary
            :outline:
            :expand:

            BuildFlow Integration


    .. grid-item-card::

        .. figure:: ../images/classyvision.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/facebookresearch/ClassyVision?style=social)]
                :target: https://github.com/facebookresearch/ClassyVision

            Classy Vision is a new end-to-end, PyTorch-based framework for large-scale training of state-of-the-art image and video classification models. The library features a modular, flexible design that allows anyone to train machine learning models on top of PyTorch using very simple abstractions.

        +++
        .. button-link:: https://github.com/facebookresearch/ClassyVision/blob/main/tutorials/ray_aws.ipynb
            :color: primary
            :outline:
            :expand:

            Classy Vision Integration


    .. grid-item-card::

        .. figure:: ../images/dask.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/dask/dask?style=social)]
                :target: https://github.com/dask/dask

            Dask provides advanced parallelism for analytics, enabling performance at scale for the tools you love. Dask uses existing Python APIs and data structures to make it easy to switch between Numpy, Pandas, Scikit-learn to their Dask-powered equivalents.

        +++
        .. button-ref:: dask-on-ray
            :color: primary
            :outline:
            :expand:

            Dask Integration


    .. grid-item-card::

        .. figure:: ../images/flambe.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/asappresearch/flambe?style=social)]
                :target: https://github.com/asappresearch/flambe

            Flambé is a machine learning experimentation framework built to accelerate the entire research life cycle. Flambé’s main objective is to provide a unified interface for prototyping models, running experiments containing complex pipelines, monitoring those experiments in real-time, reporting results, and deploying a final model for inference.

        +++
        .. button-link:: https://github.com/asappresearch/flambe
            :color: primary
            :outline:
            :expand:

            Flambé Integration


    .. grid-item-card::

        .. figure:: ../images/flyte.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/flyteorg/flyte?style=social)]
                :target: https://github.com/flyteorg/flyte

            Flyte is a Kubernetes-native workflow automation platform for complex, mission-critical data and ML processes at scale. It has been battle-tested at Lyft, Spotify, Freenome, and others and is truly open-source.

        +++
        .. button-link:: https://flyte.org/
            :color: primary
            :outline:
            :expand:

            Flyte Integration


    .. grid-item-card::

        .. figure:: ../images/horovod.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/horovod/horovod?style=social)]
                :target: https://github.com/horovod/horovod

            Horovod is a distributed deep learning training framework for TensorFlow, Keras, PyTorch, and Apache MXNet. The goal of Horovod is to make distributed deep learning fast and easy to use.

        +++
        .. button-link:: https://horovod.readthedocs.io/en/stable/ray_include.html
            :color: primary
            :outline:
            :expand:

            Horovod Integration


    .. grid-item-card::

        .. figure:: ../images/hugging.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/huggingface/transformers?style=social)]
                :target: https://github.com/huggingface/transformers

            State-of-the-art Natural Language Processing for Pytorch and TensorFlow 2.0. It integrates with Ray for distributed hyperparameter tuning of transformer models.

        +++
        .. button-link:: https://huggingface.co/transformers/master/main_classes/trainer.html#transformers.Trainer.hyperparameter_search
            :color: primary
            :outline:
            :expand:

            Hugging Face Transformers Integration


    .. grid-item-card::

        .. figure:: ../images/zoo.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/intel-analytics/analytics-zoo?style=social)]
                :target: https://github.com/intel-analytics/analytics-zoo

            Analytics Zoo seamlessly scales TensorFlow, Keras and PyTorch to distributed big data (using Spark, Flink & Ray).

        +++
        .. button-link:: https://analytics-zoo.github.io/master/#ProgrammingGuide/rayonspark/
            :color: primary
            :outline:
            :expand:

            Intel Analytics Zoo Integration


    .. grid-item-card::

        .. figure:: ../images/nlu.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/JohnSnowLabs/nlu?style=social)]
                :target: https://github.com/JohnSnowLabs/nlu

            The power of 350+ pre-trained NLP models, 100+ Word Embeddings, 50+ Sentence Embeddings, and 50+ Classifiers in 46 languages with 1 line of Python code.

        +++
        .. button-link:: https://nlu.johnsnowlabs.com/docs/en/predict_api#modin-dataframe
            :color: primary
            :outline:
            :expand:

            NLU Integration


    .. grid-item-card::

        .. figure:: ../images/ludwig.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/ludwig-ai/ludwig?style=social)]
                :target: https://github.com/ludwig-ai/ludwig

            Ludwig is a toolbox that allows users to train and test deep learning models without the need to write code. With Ludwig, you can train a deep learning model on Ray in zero lines of code, automatically leveraging Dask on Ray for data preprocessing, Horovod on Ray for distributed training, and Ray Tune for hyperparameter optimization.

        +++
        .. button-link:: https://medium.com/ludwig-ai/ludwig-ai-v0-4-introducing-declarative-mlops-with-ray-dask-tabnet-and-mlflow-integrations-6509c3875c2e
            :color: primary
            :outline:
            :expand:

            Ludwig Integration


    .. grid-item-card::

        .. figure:: ../images/mars.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/mars-project/mars?style=social)]
                :target: https://github.com/mars-project/mars

            Mars is a tensor-based unified framework for large-scale data computation which scales Numpy, Pandas and Scikit-learn. Mars can scale in to a single machine, and scale out to a cluster with thousands of machines.

        +++
        .. button-ref:: mars-on-ray
            :color: primary
            :outline:
            :expand:

            MARS Integration


    .. grid-item-card::

        .. figure:: ../images/modin.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/modin-project/modin?style=social)]
                :target: https://github.com/modin-project/modin

            Scale your pandas workflows by changing one line of code. Modin transparently distributes the data and computation so that all you need to do is continue using the pandas API as you were before installing Modin.

        +++
        .. button-link:: https://github.com/modin-project/modin
            :color: primary
            :outline:
            :expand:

            Modin Integration


    .. grid-item-card::

        .. figure:: ../images/prefect.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/PrefectHQ/prefect-ray?style=social)]
                :target: https://github.com/PrefectHQ/prefect-ray

            Prefect is an open source workflow orchestration platform in Python. It allows you to easily define, track and schedule workflows in Python. This integration makes it easy to run a Prefect workflow on a Ray cluster in a distributed way.

        +++
        .. button-link:: https://github.com/PrefectHQ/prefect-ray
            :color: primary
            :outline:
            :expand:

            Prefect Integration


    .. grid-item-card::

        .. figure:: ../images/pycaret.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/pycaret/pycaret?style=social)]
                :target: https://github.com/pycaret/pycaret

            PyCaret is an open source low-code machine learning library in Python that aims to reduce the hypothesis to insights cycle time in a ML experiment. It enables data scientists to perform end-to-end experiments quickly and efficiently.

        +++
        .. button-link:: https://github.com/pycaret/pycaret
            :color: primary
            :outline:
            :expand:

            PyCaret Integration


    .. grid-item-card::

        .. figure:: ../images/intel.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/Intel-bigdata/oap-raydp?style=social)]
                :target: https://github.com/Intel-bigdata/oap-raydp

            RayDP ("Spark on Ray") enables you to easily use Spark inside a Ray program. You can use Spark to read the input data, process the data using SQL, Spark DataFrame, or Pandas (via Koalas) API, extract and transform features using Spark MLLib, and use RayDP Estimator API for distributed training on the preprocessed dataset.

        +++
        .. button-link:: https://github.com/Intel-bigdata/oap-raydp
            :color: primary
            :outline:
            :expand:

            RayDP Integration


    .. grid-item-card::

        .. figure:: ../images/scikit.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/scikit-learn/scikit-learn?style=social)]
                :target: https://github.com/scikit-learn/scikit-learn

            Scikit-learn is a free software machine learning library for the Python programming language. It features various classification, regression and clustering algorithms including support vector machines, random forests, gradient boosting, k-means and DBSCAN, and is designed to interoperate with the Python numerical and scientific libraries NumPy and SciPy.

        +++
        .. button-link:: https://docs.ray.io/en/master/joblib.html
            :color: primary
            :outline:
            :expand:

            Scikit Learn Integration


    .. grid-item-card::

        .. figure:: ../images/seldon.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/SeldonIO/alibi?style=social)]
                :target: https://github.com/SeldonIO/alibi

            Alibi is an open source Python library aimed at machine learning model inspection and interpretation. The focus of the library is to provide high-quality implementations of black-box, white-box, local and global explanation methods for classification and regression models.

        +++
        .. button-link:: https://github.com/SeldonIO/alibi
            :color: primary
            :outline:
            :expand:

            Seldon Alibi Integration


    .. grid-item-card::

        .. figure:: ../images/sematic.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/sematic-ai/sematic?style=social)]
                :target: https://github.com/sematic-ai/sematic

            Sematic is an open-source ML pipelining tool written in Python. It enables users to write end-to-end pipelines that can seamlessly transition between your laptop and the cloud, with rich visualizations, traceability, reproducibility, and usability as first-class citizens. This integration enables dynamic allocation of Ray clusters within Sematic pipelines.

        +++
        .. button-link:: https://docs.sematic.dev/integrations/ray
            :color: primary
            :outline:
            :expand:

            Sematic Integration


    .. grid-item-card::

        .. figure:: ../images/spacy.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/explosion/spacy-ray?style=social)]
                :target: https://github.com/explosion/spacy-ray

            spaCy is a library for advanced Natural Language Processing in Python and Cython. It's built on the very latest research, and was designed from day one to be used in real products.

        +++
        .. button-link:: https://pypi.org/project/spacy-ray/
            :color: primary
            :outline:
            :expand:

            spaCy Integration


    .. grid-item-card::

        .. figure:: ../images/xgboost_logo.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/ray-project/xgboost_ray?style=social)]
                :target: https://github.com/ray-project/xgboost_ray

            XGBoost is a popular gradient boosting library for classification and regression. It is one of the most popular tools in data science and workhorse of many top-performing Kaggle kernels.

        +++
        .. button-link:: https://github.com/ray-project/xgboost_ray
            :color: primary
            :outline:
            :expand:

            XGBoost Integration


    .. grid-item-card::

        .. figure:: ../images/lightgbm_logo.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/ray-project/lightgbm_ray?style=social)]
                :target: https://github.com/ray-project/lightgbm_ray

            LightGBM is a high-performance gradient boosting library for classification and regression. It is designed to be distributed and efficient.

        +++
        .. button-link:: https://github.com/ray-project/lightgbm_ray
            :color: primary
            :outline:
            :expand:

            LightGBM Integration


    .. grid-item-card::

        .. figure:: ./images/volcano.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/volcano-sh/volcano?style=social)]
                :target: https://github.com/volcano-sh/volcano/

            Volcano is system for running high-performance workloads on Kubernetes. It features powerful batch scheduling capabilities required by ML and other data-intensive workloads.

        +++
        .. button-link:: https://github.com/volcano-sh/volcano/releases/tag/v1.7.0
            :color: primary
            :outline:
            :expand:

            Volcano Integration
