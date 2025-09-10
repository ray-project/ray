.. _working-with-ai:

Working with AI & Machine Learning: Complete Guide
==================================================

**Keywords:** artificial intelligence, machine learning, deep learning, neural networks, model training, batch inference, computer vision, natural language processing, multimodal AI, PyTorch, TensorFlow, HuggingFace, GPU acceleration

**Navigation:** :ref:`Ray Data <data>` → :ref:`User Guides <data_user_guide>` → Working with AI & Machine Learning

Ray Data excels at AI and machine learning workloads as one of its key strengths, providing native support for multimodal data processing, GPU acceleration, and seamless integration with popular ML frameworks. While Ray Data handles all types of data processing workloads, AI and ML represent some of its most advanced capabilities.

**AI/ML Workload Support Matrix**

:::list-table
   :header-rows: 1

- - **AI/ML Task**
  - **Ray Data Capability**
  - **Key Benefits**
  - **Supported Frameworks**
- - Training Data Preparation
  - Native multimodal preprocessing
  - Unified data pipeline for all data types
  - PyTorch, TensorFlow, HuggingFace
- - Batch Model Inference
  - GPU-optimized batch processing
  - Superior GPU utilization and throughput
  - vLLM, PyTorch, TensorFlow, ONNX
- - Computer Vision
  - Native image/video processing
  - Built-in media format support
  - OpenCV, PIL, Torchvision
- - Natural Language Processing
  - Large-scale text processing
  - Streaming execution for large corpora
  - HuggingFace, spaCy, NLTK
- - Multimodal AI
  - Cross-modal data processing
  - Unique capability for mixed data types
  - Custom models, research frameworks

:::

**What you'll learn:**

* Training data preparation for machine learning models
* Batch inference at scale with GPU optimization
* Multimodal data processing for AI applications
* Integration with PyTorch, TensorFlow, HuggingFace, and other ML frameworks

Why Ray Data for AI & ML
-------------------------

Ray Data was built from the ground up to handle the unique requirements of AI and machine learning workloads:

**Multimodal Data Excellence**
Native support for images, audio, video, text, and tabular data with unified processing of diverse data types in single workflows.

**Framework Integration**
First-class support for PyTorch, TensorFlow, HuggingFace, vLLM with seamless data flow between preprocessing and model training/inference.

**Scalable Performance**
Streaming execution for datasets larger than memory with heterogeneous compute and intelligent CPU/GPU allocation.

**Production Ready**
Battle-tested at companies like ByteDance (200TB+ daily), Pinterest, and Spotify with enterprise-grade monitoring and error handling.

Training Data Preparation
-------------------------

**Image Data Preparation**

Ray Data excels at preparing image data for machine learning with GPU-accelerated preprocessing and efficient batch operations.

**Step 1: Load Images**

.. code-block:: python

    import ray

    # Load image dataset using Ray Data native image reader
    images = ray.data.read_images("s3://dataset/images/")

**Step 2: Extract Labels from File Paths**

Use `map` for label extraction since it's a simple row-level operation.

.. code-block:: python

    def extract_label_from_path(row):
        """Extract training label from image file path."""
        path = row["path"]
        # Extract label from directory name (e.g., /cats/image1.jpg -> "cats")
        label = path.split("/")[-2]
        return {**row, "label": label}

    # Apply label extraction
    images_with_labels = images.map(extract_label_from_path)

**Why `map` for labels:** Label extraction is a simple string operation that doesn't require batch processing or GPU resources.

**Step 3: Preprocess Images for Training**

Use `map_batches` for image preprocessing since it benefits from GPU acceleration and batch processing.

.. code-block:: python

    def preprocess_image_batch(batch):
        """Preprocess images using GPU-accelerated operations."""
        from PIL import Image
        import torchvision.transforms as transforms
        
        # Define preprocessing pipeline
        transform = transforms.Compose([
            transforms.Resize((224, 224)),
            transforms.RandomHorizontalFlip(p=0.5),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
        ])
        
        # Process images in batch
        processed_images = []
        for img_array in batch["image"]:
            image = Image.fromarray(img_array)
            processed_tensor = transform(image)
            processed_images.append(processed_tensor.numpy())
        
        # Update batch with processed images
        batch["processed_image"] = processed_images
        return batch

    # Apply preprocessing with GPU acceleration
    processed_images = images_with_labels.map_batches(
        preprocess_image_batch,
        concurrency=4,  # Use 4 actors for parallel processing
        num_gpus=1,  # Allocate 1 GPU per actor for image processing
        batch_size=32  # Optimize batch size for GPU memory (224x224 images)
    )

**Resource allocation rationale:** 
- **GPU allocation**: 1 GPU per actor provides optimal resource utilization for image preprocessing
- **Concurrency**: `concurrency=4` creates exactly 4 actors (for classes) vs at most 4 tasks (for functions)
- **Batch size**: 32 images per batch optimizes GPU memory usage for 224x224 images (~50MB per batch)

**Note:** When using callable classes (like `preprocess_image_batch`), `concurrency=n` creates exactly n actors. For functions, it creates at most n concurrent tasks.

**Text Data Preparation**

.. code-block:: python

    import ray
    from transformers import AutoTokenizer

    # Load text dataset
    text_data = ray.data.read_json("s3://dataset/text-data.jsonl")

    def preprocess_text_for_training(batch):
        """Preprocess text data for language model training."""
        tokenizer = AutoTokenizer.from_pretrained("bert-base-uncased")
        processed_texts = []
        
        for item in batch.to_pylist():
            text = item["text"]
            label = item.get("label", 0)
            
            # Tokenize text
            encoded = tokenizer(
                text,
                truncation=True,
                padding="max_length",
                max_length=512,
                return_tensors="np"
            )
            
            processed_texts.append({
                "input_ids": encoded["input_ids"].flatten(),
                "attention_mask": encoded["attention_mask"].flatten(),
                "label": label
            })
        
        return pd.DataFrame(processed_texts)

    processed_text = text_data.map_batches(preprocess_text_for_training)

Batch Inference at Scale
------------------------

**Image Classification Inference**

.. code-block:: python

    import ray
    import torch
    import torchvision.models as models

    # Initialize model
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = models.resnet50(pretrained=True)
    model.eval()
    model.to(device)

    def classify_images(batch):
        """Run inference on a batch of images."""
        predictions = []
            
            with torch.no_grad():
                for item in batch.to_pylist():
                    # Preprocess and run inference
                    image = preprocess_image(item["image"]).to(self.device)
                    outputs = self.model(image)
                    probabilities = torch.nn.functional.softmax(outputs[0], dim=0)
                    
                    top_prob, top_class = torch.topk(probabilities, 1)
                    
                    predictions.append({
                        "image_path": item.get("path", ""),
                        "predicted_class": top_class.item(),
                        "confidence": top_prob.item()
                    })
            
            return pd.DataFrame(predictions)

    # Load images and run inference
    test_images = ray.data.read_images("s3://test-dataset/images/")
    
    predictions = test_images.map_batches(
        ImageClassifier,
        concurrency=4,  # Use 4 actors for parallel image classification
        num_gpus=1,
        batch_size=32
    )

**Large Language Model Inference**

.. code-block:: python

    import ray
    from transformers import AutoTokenizer, AutoModelForCausalLM

    class LLMInference:
        """Large language model inference with GPU acceleration."""
        
        def __init__(self, model_name="gpt2-medium"):
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            self.tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.model = AutoModelForCausalLM.from_pretrained(model_name)
            self.model.to(self.device)
            self.model.eval()
        
        def __call__(self, batch):
            """Generate text completions for a batch of prompts."""
            completions = []
            
            for item in batch.to_pylist():
                prompt = item["prompt"]
                
                # Tokenize and generate
                inputs = self.tokenizer(prompt, return_tensors="pt").to(self.device)
                
                with torch.no_grad():
                    outputs = self.model.generate(
                        **inputs,
                        max_new_tokens=100,
                        temperature=0.7,
                        do_sample=True
                    )
                
                completion = self.tokenizer.decode(
                    outputs[0][inputs["input_ids"].shape[1]:],
                    skip_special_tokens=True
                )
                
                completions.append({
                    "prompt": prompt,
                    "completion": completion
                })
            
            return pd.DataFrame(completions)

    # Load prompts and run inference
    prompts = ray.data.read_json("s3://prompts/batch-prompts.jsonl")
    
    completions = prompts.map_batches(
        LLMInference,
        concurrency=2,  # Use 2 actors for LLM inference
        num_gpus=2,
        batch_size=8
    )

Framework Integration
--------------------

**PyTorch Integration**

.. code-block:: python

    import ray
    import torch

    # Convert Ray Dataset to PyTorch format
    processed_data = ray.data.read_parquet("s3://processed-dataset/")
    
    torch_dataset = processed_data.to_torch(
        label_column="label",
        feature_columns=["image"],
        batch_size=32
    )

    # Use with PyTorch training
    model = YourPyTorchModel()
    for batch in torch_dataset:
        loss = model(batch["image"], batch["label"])
        loss.backward()
        optimizer.step()

**HuggingFace Integration**

.. code-block:: python

    import ray
    from transformers import Trainer, TrainingArguments
    from datasets import Dataset

    # Convert Ray Dataset to HuggingFace Dataset
    text_data = ray.data.read_json("s3://text-dataset/")
    pandas_df = text_data.to_pandas()
    hf_dataset = Dataset.from_pandas(pandas_df)

    # Use with HuggingFace Trainer
    training_args = TrainingArguments(
        output_dir="./results",
        num_train_epochs=3,
        per_device_train_batch_size=16
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=hf_dataset,
        tokenizer=tokenizer
    )

    trainer.train()

Production ML Pipeline
----------------------

**End-to-End ML Pipeline**

.. code-block:: python

    import ray
    from ray import train
    from ray.train import ScalingConfig

    def ml_pipeline():
        """Complete ML pipeline from data to model."""
        
        # 1. Data preprocessing
        raw_data = ray.data.read_parquet("s3://raw-data/")
        processed_data = raw_data.map_batches(preprocess_features)
        train_data, val_data = processed_data.random_split([0.8, 0.2])
        
        # 2. Model training
        def train_model(config):
            model = create_model(config)
            train_dataset = train_data.to_torch(batch_size=config["batch_size"])
            
            for epoch in range(config["num_epochs"]):
                for batch in train_dataset:
                    loss = model.train_step(batch)
                
                val_loss = model.evaluate(val_data)
                train.report({"val_loss": val_loss})
        
        # Configure Ray Train for distributed training
        trainer = train.TorchTrainer(
            train_model,
            train_loop_config={"batch_size": 32, "num_epochs": 10},
            scaling_config=ScalingConfig(num_workers=4, use_gpu=True)
        )
        
        # For complete Ray Train documentation:
        # :ref:`Ray Train Getting Started <train-pytorch>`
        
        result = trainer.fit()
        
        # 3. Batch inference
        test_data = ray.data.read_parquet("s3://test-data/")
        predictions = test_data.map_batches(
            lambda batch: trained_model.predict(batch),
            concurrency=4,  # Use 4 actors for parallel prediction
            num_gpus=1
        )
        
        predictions.write_parquet("s3://predictions/")
        return result, predictions

Best Practices
--------------

**GPU Optimization**
* Use `ActorPoolStrategy` with `num_gpus` for GPU acceleration
* Configure appropriate batch sizes for GPU memory
* Monitor GPU utilization and memory usage

**Data Pipeline Optimization**
* Preprocess data once and save in optimized format
* Use streaming execution for large datasets
* Implement data validation and quality checks

**Memory Management**
* Configure block sizes appropriately for your data
* Use lazy evaluation to minimize memory usage
* Implement checkpointing for long-running jobs

**Scalability Planning**
* Design pipelines to scale with data and compute resources
* Use incremental processing for growing datasets
* Plan for fault tolerance and recovery

Next Steps
----------

Explore related AI/ML topics:

* **Working with Images**: Computer vision workflows → :ref:`working-with-images`
* **Working with Text**: NLP and text processing → :ref:`working-with-text`
* **Working with PyTorch**: Deep PyTorch integration → :ref:`working-with-pytorch`
* **Working with LLMs**: Large language model workflows → :ref:`working-with-llms`