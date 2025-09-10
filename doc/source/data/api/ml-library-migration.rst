.. _ml-library-migration:

Migrating from AI/ML Libraries to Ray Data
===========================================

**Keywords:** ML library migration, PyTorch migration, TensorFlow migration, HuggingFace migration, AI data processing, ML data pipeline migration

This guide helps users of AI/ML libraries (PyTorch, TensorFlow, HuggingFace) migrate to Ray Data for scalable, distributed data preprocessing and enhanced multimodal capabilities.

**What you'll learn:**

* API mappings for popular AI/ML data libraries
* Benefits of Ray Data for AI/ML workflows
* Integration patterns with Ray Train and Ray Serve
* Scaling strategies for AI data processing

AI/ML Library Migration Overview
---------------------------------

**Why migrate AI/ML data processing to Ray Data:**
- **Distributed scaling**: Scale data preprocessing across multiple machines
- **GPU optimization**: Better GPU utilization for data preprocessing
- **Multimodal support**: Process images, text, audio, video in unified workflows
- **Ray ecosystem**: Seamless integration with Ray Train, Tune, Serve

HuggingFace Datasets Migration
------------------------------

.. list-table:: HuggingFace Datasets vs. Ray Data APIs
   :header-rows: 1

   * - HuggingFace API
     - Ray Data API
     - Ray Data Advantage
   * - ``datasets.load_dataset()``
     - :meth:`ray.data.from_huggingface() <ray.data.from_huggingface>`
     - Distributed loading, streaming execution
   * - ``dataset.map(func, batched=True)``
     - :meth:`ds.map_batches(func) <ray.data.Dataset.map_batches>`
     - GPU acceleration, advanced transformations
   * - ``dataset.filter(func)``
     - :meth:`ds.filter(func) <ray.data.Dataset.filter>`
     - Distributed filtering, memory efficiency
   * - ``dataset.train_test_split()``
     - :meth:`ds.train_test_split() <ray.data.Dataset.train_test_split>`
     - Distributed splitting, streaming execution
   * - ``dataset.shuffle()``
     - :meth:`ds.random_shuffle() <ray.data.Dataset.random_shuffle>`
     - Distributed shuffling, fault tolerance

**Migration Example:**

.. code-block:: python

    # HuggingFace approach (single-node)
    from datasets import load_dataset
    
    dataset = load_dataset("text", data_files="corpus.txt")
    tokenized = dataset.map(tokenize_function, batched=True)

    # Ray Data approach (distributed, GPU-accelerated)
    import ray
    
    dataset = ray.data.read_text("corpus.txt")
    tokenized = dataset.map_batches(
        tokenize_function,
        concurrency=8,  # Use 8 actors for distributed tokenization
        num_gpus=0.5  # GPU acceleration
    )

TensorFlow Data Migration
-------------------------

.. list-table:: TensorFlow Data vs. Ray Data APIs
   :header-rows: 1

   * - TensorFlow Data API
     - Ray Data API
     - Ray Data Advantage
   * - ``tf.data.Dataset.from_tensor_slices()``
     - :meth:`ray.data.from_numpy() <ray.data.from_numpy>`
     - Distributed processing, multimodal support
   * - ``tf.data.Dataset.map()``
     - :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`
     - GPU acceleration, flexible transformations
   * - ``tf.data.Dataset.batch()``
     - :meth:`ds.iter_batches() <ray.data.Dataset.iter_batches>`
     - Intelligent batching, resource optimization
   * - ``tf.data.Dataset.shuffle()``
     - :meth:`ds.random_shuffle() <ray.data.Dataset.random_shuffle>`
     - Distributed shuffling, memory optimization

PyTorch DataLoader Migration
----------------------------

.. list-table:: PyTorch DataLoader vs. Ray Data APIs
   :header-rows: 1

   * - PyTorch API
     - Ray Data API
     - Ray Data Advantage
   * - ``DataLoader(dataset, batch_size)``
     - :meth:`ds.iter_torch_batches(batch_size) <ray.data.Dataset.iter_torch_batches>`
     - Multi-node data loading, GPU optimization
   * - ``Dataset.__getitem__()``
     - :meth:`ds.iter_rows() <ray.data.Dataset.iter_rows>`
     - Distributed processing, streaming execution
   * - ``transforms.Compose()``
     - :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`
     - Distributed transforms, GPU acceleration

**Complete Migration Example:**

.. code-block:: python

    # PyTorch approach (single-node data loading)
    from torch.utils.data import DataLoader
    from torchvision import transforms
    
    transform = transforms.Compose([
        transforms.Resize((224, 224)),
        transforms.ToTensor()
    ])
    
    dataset = ImageDataset(image_paths, transform=transform)
    dataloader = DataLoader(dataset, batch_size=32)

    # Ray Data approach (distributed, GPU-accelerated)
    import ray
    
    def preprocess_images(batch):
        # Distributed image preprocessing with GPU
        processed = []
        for img in batch["image"]:
            resized = resize_image(img, (224, 224))
            tensor = convert_to_tensor(resized)
            processed.append(tensor)
        return {"image": processed}
    
    dataset = ray.data.read_images("s3://images/")
    processed = dataset.map_batches(
        preprocess_images,
        concurrency=4,  # Use 4 actors for image preprocessing
        num_gpus=1
    )
    
    # Iterate for training with Ray Train integration
    for batch in processed.iter_torch_batches(batch_size=32):
        # Training logic here
        pass

AI/ML Migration Best Practices
-------------------------------

**Migration Strategy:**
- **Start with data preprocessing**: Migrate data loading and preprocessing first
- **Maintain model training**: Keep existing training code while migrating data
- **Gradual integration**: Progressively adopt Ray Train for distributed training
- **Performance validation**: Benchmark preprocessing speed and GPU utilization

**Integration Benefits:**
- **Ray Train**: Seamless handoff from Ray Data preprocessing to distributed training
- **Ray Tune**: Hyperparameter optimization for both data processing and model training
- **Ray Serve**: Deploy trained models with Ray Data batch inference
- **Unified platform**: Complete ML lifecycle on single Ray platform

Next Steps
----------

**Complete Your AI/ML Migration:**
- **Framework integration**: :ref:`Framework Integration <frameworks>`
- **AI workflow patterns**: :ref:`Working with AI <working-with-ai>`
- **Real-world examples**: :ref:`AI-Powered Pipelines <ai-powered-pipelines>`
- **Production deployment**: :ref:`Model Training Pipelines <model-training-pipelines>`
