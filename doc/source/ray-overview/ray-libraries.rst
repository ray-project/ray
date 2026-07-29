.. meta::
   :description: Third-party libraries and tools that integrate with Ray for distributed execution, plus an ecosystem map showing component maturity.

.. _ray-oss-list:

The Ray Ecosystem
=================

This page lists libraries that have integrations with Ray for distributed execution
in alphabetical order.
It's easy to add your own integration to this list.
Simply open a pull request with a few lines of text, see the dropdown below for
more information.

.. dropdown:: Adding Your Integration

    To add an integration add an entry to this file, using the same
    ``grid-item-card`` directive that the other examples use.

.. grid:: 1 2 2 3
    :gutter: 1
    :class-container: container pb-3

    .. grid-item-card::

        .. figure:: ../images/aibrix.jpeg
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/vllm-project/aibrix?style=social
                :target: https://github.com/vllm-project/aibrix

            AIBrix is a cloud-native LLM inference infrastructure platform that provides building blocks for deploying, scaling, and optimizing large language model serving with Ray-based hybrid orchestration.

        +++
        .. button-link:: https://github.com/vllm-project/aibrix
            :color: primary
            :outline:
            :expand:

            AIBrix Integration


    .. grid-item-card::

        .. figure:: ../images/areal.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/areal-project/AReaL?style=social
                :target: https://github.com/areal-project/AReaL

            AReaL is an asynchronous reinforcement learning system for LLM agents developed by Ant Group. It decouples generation from training for efficient distributed post-training on Ray clusters.

        +++
        .. button-link:: https://github.com/areal-project/AReaL
            :color: primary
            :outline:
            :expand:

            AReaL Integration


    .. grid-item-card::

        .. figure:: ../images/cosmos_curate.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/NVIDIA/cosmos-curator?style=social
                :target: https://github.com/NVIDIA/cosmos-curator

            Cosmos Curate is a GPU-accelerated video and image data curation toolkit from NVIDIA. It provides scalable pipelines for filtering, deduplication, and quality scoring using Ray for multi-node, multi-GPU distributed processing.

        +++
        .. button-link:: https://github.com/NVIDIA/cosmos-curator
            :color: primary
            :outline:
            :expand:

            Cosmos Curate Integration


    .. grid-item-card::

        .. figure:: ../images/daft.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/Eventual-Inc/Daft?style=social
                :target: https://github.com/Eventual-Inc/Daft

            Daft is a high-performance multimodal data engine that provides simple and reliable data processing for any modality - from structured tables to images, audio, video, and embeddings. Built with Python and Rust for modern AI workflows, Daft offers seamless scaling from local to distributed clusters, enabling efficient batch inference, document processing, and multimodal ETL pipelines at scale.

        +++
        .. button-link:: https://docs.daft.ai/en/stable/distributed/ray/
            :color: primary
            :outline:
            :expand:

            Daft Integration


    .. grid-item-card::

        .. figure:: ../images/data_juicer.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/modelscope/data-juicer?style=social
                :target: https://github.com/modelscope/data-juicer

            Data-Juicer is a one-stop multimodal data processing system to make data higher-quality, juicier, and more digestible for foundation models. It integrates with Ray for distributed data processing on large-scale datasets with over 100 multimodal operators and supports TB-size dataset deduplication.

        +++
        .. button-link:: https://github.com/modelscope/data-juicer?tab=readme-ov-file#distributed-data-processing
            :color: primary
            :outline:
            :expand:

            Data-Juicer Integration


    .. grid-item-card::

        .. figure:: ../images/deltacat.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/ray-project/deltacat?style=social
                :target: https://github.com/ray-project/deltacat

            DeltaCAT is a portable multimodal lakehouse powered by Ray for petabyte-scale data compaction, deduplication, and incremental table processing with ACID compliance.

        +++
        .. button-link:: https://github.com/ray-project/deltacat
            :color: primary
            :outline:
            :expand:

            DeltaCAT Integration


    .. grid-item-card::

        .. figure:: ../images/modin.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/modin-project/modin?style=social
                :target: https://github.com/modin-project/modin

            Scale your pandas workflows by changing one line of code. Modin transparently distributes the data and computation so that all you need to do is continue using the pandas API as you were before installing Modin.

        +++
        .. button-link:: https://github.com/modin-project/modin
            :color: primary
            :outline:
            :expand:

            Modin Integration


    .. grid-item-card::

        .. figure:: ../images/nemo.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/NVIDIA-NeMo/Curator?style=social
                :target: https://github.com/NVIDIA-NeMo/Curator

            NeMo Curator is a scalable data curation toolkit from NVIDIA for preparing high-quality datasets for large language model training. It uses Ray for distributed data processing including deduplication, filtering, and quality classification at scale.

        +++
        .. button-link:: https://github.com/NVIDIA-NeMo/Curator
            :color: primary
            :outline:
            :expand:

            NeMo Curator Integration


    .. grid-item-card::

        .. figure:: ../images/nemo.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/NVIDIA-NeMo/RL?style=social
                :target: https://github.com/NVIDIA-NeMo/RL

            NeMo-RL is NVIDIA's scalable post-training toolkit for large language models. It provides RLHF and alignment training built on Ray for distributed orchestration of training and inference workloads.

        +++
        .. button-link:: https://github.com/NVIDIA-NeMo/RL
            :color: primary
            :outline:
            :expand:

            NeMo-RL Integration


    .. grid-item-card::

        .. figure:: ../images/openrlhf.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/OpenRLHF/OpenRLHF?style=social
                :target: https://github.com/OpenRLHF/OpenRLHF

            OpenRLHF is an easy-to-use, scalable RLHF training framework. It supports distributed PPO, DPO, rejection sampling, and other alignment methods using Ray for orchestrating training and generation across multiple GPUs and nodes.

        +++
        .. button-link:: https://github.com/OpenRLHF/OpenRLHF
            :color: primary
            :outline:
            :expand:

            OpenRLHF Integration


    .. grid-item-card::

        .. figure:: ../images/intel.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/Intel-bigdata/oap-raydp?style=social
                :target: https://github.com/Intel-bigdata/oap-raydp

            RayDP ("Spark on Ray") enables you to easily use Spark inside a Ray program. You can use Spark to read the input data, process the data using SQL, Spark DataFrame, or Pandas (via Koalas) API, extract and transform features using Spark MLLib, and use RayDP Estimator API for distributed training on the preprocessed dataset.

        +++
        .. button-link:: https://github.com/Intel-bigdata/oap-raydp
            :color: primary
            :outline:
            :expand:

            RayDP Integration


    .. grid-item-card::

        .. figure:: ../images/roll.jpg
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/alibaba/ROLL?style=social
                :target: https://github.com/alibaba/ROLL

            ROLL is Alibaba's reinforcement learning scaling library for large language models. It provides efficient distributed RL training with flexible resource scheduling and heterogeneous task management built on Ray.

        +++
        .. button-link:: https://github.com/alibaba/ROLL
            :color: primary
            :outline:
            :expand:

            ROLL Integration


    .. grid-item-card::

        .. figure:: ../images/skyrl.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/NovaSky-AI/SkyRL?style=social
                :target: https://github.com/NovaSky-AI/SkyRL

            SkyRL is a modular reinforcement learning library for LLM agents from UC Berkeley. It enables training through multi-turn environment interactions using Ray for distributed rollout and training.

        +++
        .. button-link:: https://github.com/NovaSky-AI/SkyRL
            :color: primary
            :outline:
            :expand:

            SkyRL Integration


    .. grid-item-card::

        .. figure:: ../images/slime.jpg
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/THUDM/slime?style=social
                :target: https://github.com/THUDM/slime

            SLIME is a post-training framework for large language models from Tsinghua University. It provides RL scaling with a service-oriented architecture built on Ray and Megatron-LM for distributed training orchestration.

        +++
        .. button-link:: https://github.com/THUDM/slime
            :color: primary
            :outline:
            :expand:

            SLIME Integration


    .. grid-item-card::

        .. figure:: ../images/syftr.jpg
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/datarobot/syftr?style=social
                :target: https://github.com/datarobot/syftr

            Syftr is an open-source agent workflow optimizer from DataRobot. It uses Ray and Ray Tune for scalable multi-objective optimization of agentic AI workflows across prompts, models, and tool selections.

        +++
        .. button-link:: https://github.com/datarobot/syftr
            :color: primary
            :outline:
            :expand:

            Syftr Integration


    .. grid-item-card::

        .. figure:: ../images/verl.jpg
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/verl-project/verl?style=social
                :target: https://github.com/verl-project/verl

            verl is a flexible and efficient reinforcement learning training library for large language models from ByteDance. It provides a Ray-native hybrid controller for scalable RLHF training with distributed orchestration of rollout, training, and reward computation.

        +++
        .. button-link:: https://github.com/verl-project/verl
            :color: primary
            :outline:
            :expand:

            verl Integration


    .. grid-item-card::

        .. figure:: ../images/vllm.png
            :class: card-figure

        .. div::

            .. image:: https://img.shields.io/github/stars/vllm-project/vllm?style=social
                :target: https://github.com/vllm-project/vllm

            vLLM is a high-throughput and memory-efficient inference and serving engine for large language models. It uses Ray for distributed tensor parallelism and pipeline parallelism across multiple GPUs and nodes.

        +++
        .. button-link:: https://docs.vllm.ai/
            :color: primary
            :outline:
            :expand:

            vLLM Integration
