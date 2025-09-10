.. _working_with_text:

Working with Text
=================

**Navigation:** :ref:`Ray Data <data>` → :ref:`User Guides <data_user_guide>` → Working with Text

**Learning Path:** This guide is part of Ray Data's **multimodal processing capabilities**. After completing this guide, explore :ref:`Working with LLMs <working-with-llms>` or :ref:`Working with AI <working-with-ai>` for advanced NLP workflows.

Ray Data provides comprehensive support for text processing workloads, from simple text transformations to complex natural language processing pipelines. This guide shows you how to efficiently work with text datasets of any scale.

**What you'll learn:**

* Loading text data from various sources and formats
* Performing text transformations and preprocessing
* Building NLP pipelines with advanced language models
* Integrating with popular NLP frameworks
* Optimizing text processing for production workloads

Why Ray Data for Text Processing
--------------------------------

Ray Data excels at text processing workloads through several key advantages:

**Multimodal Data Excellence**
Native support for text alongside other data types in unified workflows, enabling complex multimodal AI applications.

**Scalable Performance**
Process text datasets larger than memory with streaming execution and intelligent resource allocation.

**NLP Framework Integration**
Seamless integration with Hugging Face, spaCy, NLTK, and other popular NLP libraries.

**Production Ready**
Battle-tested at companies processing millions of documents daily with enterprise-grade monitoring and error handling.

Loading Text Data
-----------------

Ray Data supports loading text data from multiple sources and formats with automatic encoding detection and optimization.

**Text File Formats**

Ray Data can read text from various formats using different read functions:

.. code-block:: python

    import ray

    # Load plain text files (one line per row)
    text_data = ray.data.read_text("data/documents/")

    # Load text from cloud storage
    cloud_text = ray.data.read_text("s3://bucket/text-dataset/")

    # Load with specific encoding
    utf8_text = ray.data.read_text("data/documents/", encoding="utf-8")

    # Load with path information
    text_with_paths = ray.data.read_text(
        "s3://bucket/documents/",
        include_paths=True
    )

**JSON Lines (JSONL)**

Load structured text data stored in JSON Lines format:

.. code-block:: python

    # Load JSON Lines files
    jsonl_data = ray.data.read_json("s3://bucket/documents.jsonl")

    # Load with schema validation
    validated_data = ray.data.read_json(
        "s3://bucket/documents.jsonl",
        schema=pa.schema([
            ("text", pa.string()),
            ("label", pa.int64()),
            ("timestamp", pa.timestamp('s'))
        ])
    )

**CSV with Text Columns**

Load text data from CSV files:

.. code-block:: python

    # Load CSV with text columns
    csv_text = ray.data.read_csv("s3://bucket/text-dataset.csv")

    # Load with specific text columns
    text_columns = ray.data.read_csv(
        "s3://bucket/documents.csv",
        columns=["content", "title", "author"]
    )

**Binary Files with Custom Decoding**

Load and decode text from binary files:

.. code-block:: python

    from typing import Any, Dict
    from bs4 import BeautifulSoup
    import ray

    def parse_html(row: Dict[str, Any]) -> Dict[str, Any]:
        """Parse HTML content and extract text."""
        html = row["bytes"].decode("utf-8")
        soup = BeautifulSoup(html, features="html.parser")
        
        # Extract text content
        text = soup.get_text().strip()
        
        # Extract metadata
        title = soup.title.string if soup.title else ""
        links = [a.get("href") for a in soup.find_all("a")]
        
        return {
            "text": text,
            "title": title,
            "links": links,
            "word_count": len(text.split())
        }

    # Load and parse HTML files
    html_documents = (
        ray.data.read_text("s3://bucket/html-documents/")
        .map(parse_html)
    )

Text Transformations
--------------------

Transform text data using Ray Data's powerful transformation capabilities with support for complex NLP operations.

**Basic Text Transformations**

.. code-block:: python

    import re
    from typing import Dict, Any
    import ray

    # Load text dataset
    text_data = ray.data.read_text("s3://bucket/documents/")

    def basic_text_cleaning(batch: Dict[str, Any]) -> Dict[str, Any]:
        """Apply basic text cleaning operations."""
        
        cleaned_texts = []
        
        for text in batch["text"]:
            # Convert to lowercase
            text = text.lower()
            
            # Remove extra whitespace
            text = re.sub(r'\s+', ' ', text)
            
            # Remove special characters (keep alphanumeric and spaces)
            text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
            
            # Strip leading/trailing whitespace
            text = text.strip()
            
            cleaned_texts.append(text)
        
        batch["cleaned_text"] = cleaned_texts
        batch["original_length"] = [len(t) for t in batch["text"]]
        batch["cleaned_length"] = [len(t) for t in cleaned_texts]
        
        return batch

    # Apply basic cleaning
    cleaned_data = text_data.map_batches(basic_text_cleaning)

**Advanced Text Processing**

.. code-block:: python

    import nltk
    from nltk.tokenize import word_tokenize, sent_tokenize
    from nltk.corpus import stopwords
    from nltk.stem import WordNetLemmatizer
    import ray

    # Download required NLTK data
    nltk.download('punkt')
    nltk.download('stopwords')
    nltk.download('wordnet')

    class AdvancedTextProcessor:
        """Advanced text processing with NLTK."""
        
        def __init__(self):
            self.stop_words = set(stopwords.words('english'))
            self.lemmatizer = WordNetLemmatizer()
        
        def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
            """Apply advanced text processing."""
            
            processed_texts = []
            tokenized_texts = []
            lemmatized_texts = []
            
            for text in batch["text"]:
                # Tokenize into sentences
                sentences = sent_tokenize(text)
                
                # Tokenize into words
                words = word_tokenize(text.lower())
                
                # Remove stopwords and lemmatize
                filtered_words = [
                    self.lemmatizer.lemmatize(word) 
                    for word in words 
                    if word.isalnum() and word not in self.stop_words
                ]
                
                # Join processed text
                processed_text = ' '.join(filtered_words)
                
                processed_texts.append(processed_text)
                tokenized_texts.append(words)
                lemmatized_texts.append(filtered_words)
            
            batch["processed_text"] = processed_texts
            batch["tokenized"] = tokenized_texts
            batch["lemmatized"] = lemmatized_texts
            batch["sentence_count"] = [len(sent_tokenize(t)) for t in batch["text"]]
            batch["word_count"] = [len(t.split()) for t in batch["text"]]
            
            return batch

    # Apply advanced processing
    advanced_processed = text_data.map_batches(AdvancedTextProcessor())

**Text Feature Engineering**

.. code-block:: python

    import numpy as np
    from typing import Dict, Any
    import ray

    def extract_text_features(batch: Dict[str, Any]) -> Dict[str, Any]:
        """Extract various text features for analysis."""
        
        features = []
        
        for text in batch["text"]:
            # Basic statistics
            char_count = len(text)
            word_count = len(text.split())
            sentence_count = len(text.split('.'))
            
            # Readability metrics
            avg_word_length = np.mean([len(word) for word in text.split()]) if word_count > 0 else 0
            avg_sentence_length = word_count / sentence_count if sentence_count > 0 else 0
            
            # Vocabulary diversity
            unique_words = len(set(text.lower().split()))
            vocabulary_diversity = unique_words / word_count if word_count > 0 else 0
            
            # Sentiment indicators (simple heuristics)
            positive_words = len([w for w in text.lower().split() if w in ['good', 'great', 'excellent', 'amazing']])
            negative_words = len([w for w in text.lower().split() if w in ['bad', 'terrible', 'awful', 'horrible']])
            
            features.append({
                "char_count": char_count,
                "word_count": word_count,
                "sentence_count": sentence_count,
                "avg_word_length": avg_word_length,
                "avg_sentence_length": avg_sentence_length,
                "vocabulary_diversity": vocabulary_diversity,
                "positive_words": positive_words,
                "negative_words": negative_words,
                "sentiment_score": positive_words - negative_words
            })
        
        # Add features to batch
        for key in features[0].keys():
            batch[key] = [f[key] for f in features]
        
        return batch

    # Extract text features
    featured_data = text_data.map_batches(extract_text_features)

Natural Language Processing Pipelines
------------------------------------

Build end-to-end NLP pipelines with Ray Data for text analysis, classification, and generation.

**Text Classification Pipeline**

.. code-block:: python

    from transformers import AutoTokenizer, AutoModelForSequenceClassification
    import torch
    from typing import Dict, Any
    import ray

    class TextClassifier:
        """Text classification with pre-trained models."""
        
        def __init__(self, model_name="distilbert-base-uncased"):
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            
            # Load tokenizer and model
            self.tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
            
            self.model.eval()
            self.model.to(self.device)
            
            # Define class labels
            self.labels = ["negative", "positive", "neutral"]
        
        def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
            """Classify texts in batch."""
            
            predictions = []
            confidences = []
            
            for text in batch["text"]:
                # Tokenize text
                inputs = self.tokenizer(
                    text,
                    truncation=True,
                    padding=True,
                    max_length=512,
                    return_tensors="pt"
                ).to(self.device)
                
                # Run inference
                with torch.no_grad():
                    outputs = self.model(**inputs)
                    probabilities = torch.nn.functional.softmax(outputs.logits, dim=1)
                    
                    # Get prediction and confidence
                    top_prob, top_class = torch.topk(probabilities, 1)
                    
                    predictions.append(self.labels[top_class.item()])
                    confidences.append(top_prob.item())
            
            batch["predicted_class"] = predictions
            batch["confidence"] = confidences
            
            return batch

    # Build classification pipeline
    classification_pipeline = (
        text_data
        .map_batches(basic_text_cleaning)
        .map_batches(
            TextClassifier,
            compute=ray.data.ActorPoolStrategy(size=2),
            num_gpus=1
        )
    )

**Named Entity Recognition Pipeline**

.. code-block:: python

    import spacy
    from typing import Dict, Any, List
    import ray

    class NamedEntityRecognizer:
        """Named Entity Recognition with spaCy."""
        
        def __init__(self, model_name="en_core_web_sm"):
            self.nlp = spacy.load(model_name)
        
        def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
            """Extract named entities from texts."""
            
            entity_results = []
            
            for text in batch["text"]:
                # Process text with spaCy
                doc = self.nlp(text)
                
                # Extract entities
                entities = []
                for ent in doc.ents:
                    entities.append({
                        "text": ent.text,
                        "label": ent.label_,
                        "start": ent.start_char,
                        "end": ent.end_char
                    })
                
                # Group entities by type
                entity_types = {}
                for entity in entities:
                    label = entity["label"]
                    if label not in entity_types:
                        entity_types[label] = []
                    entity_types[label].append(entity["text"])
                
                entity_results.append({
                    "entities": entities,
                    "entity_types": entity_types,
                    "entity_count": len(entities)
                })
            
            batch["ner_results"] = entity_results
            return batch

    # Build NER pipeline
    ner_pipeline = (
        text_data
        .map_batches(basic_text_cleaning)
        .map_batches(NamedEntityRecognizer())
    )

**Text Summarization Pipeline**

.. code-block:: python

    from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
    import torch
    from typing import Dict, Any
    import ray

    class TextSummarizer:
        """Text summarization with pre-trained models."""
        
        def __init__(self, model_name="facebook/bart-large-cnn"):
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            
            # Load tokenizer and model
            self.tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.model = AutoModelForSeq2SeqLM.from_pretrained(model_name)
            
            self.model.eval()
            self.model.to(self.device)
        
        def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
            """Summarize texts in batch."""
            
            summaries = []
            
            for text in batch["text"]:
                # Truncate text if too long
                if len(text) > 1000:
                    text = text[:1000]
                
                # Tokenize input
                inputs = self.tokenizer(
                    text,
                    truncation=True,
                    max_length=1024,
                    return_tensors="pt"
                ).to(self.device)
                
                # Generate summary
                with torch.no_grad():
                    summary_ids = self.model.generate(
                        inputs["input_ids"],
                        max_length=150,
                        min_length=40,
                        length_penalty=2.0,
                        num_beams=4,
                        early_stopping=True
                    )
                
                # Decode summary
                summary = self.tokenizer.decode(summary_ids[0], skip_special_tokens=True)
                summaries.append(summary)
            
            batch["summary"] = summaries
            batch["summary_length"] = [len(s.split()) for s in summaries]
            
            return batch

    # Build summarization pipeline
    summarization_pipeline = (
        text_data
        .map_batches(basic_text_cleaning)
        .map_batches(
            TextSummarizer,
            compute=ray.data.ActorPoolStrategy(size=2),
            num_gpus=1
        )
    )

Large Language Model Integration
-------------------------------

Integrate Ray Data with large language models for advanced text processing tasks.

**Hugging Face Integration**

.. code-block:: python

    from transformers import pipeline, AutoTokenizer, AutoModelForCausalLM
    import torch
    from typing import Dict, Any
    import ray

    class HuggingFaceTextProcessor:
        """Text processing with Hugging Face models."""
        
        def __init__(self):
            # Load sentiment analysis pipeline
            self.sentiment_analyzer = pipeline("sentiment-analysis")
            
            # Load text generation model
            self.tokenizer = AutoTokenizer.from_pretrained("gpt2")
            self.model = AutoModelForCausalLM.from_pretrained("gpt2")
            
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            self.model.to(self.device)
        
        def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
            """Process texts with multiple Hugging Face models."""
            
            sentiment_results = []
            generated_texts = []
            
            for text in batch["text"]:
                # Sentiment analysis
                sentiment = self.sentiment_analyzer(text[:500])[0]  # Limit length
                sentiment_results.append(sentiment)
                
                # Text generation
                inputs = self.tokenizer.encode(text[:100], return_tensors="pt").to(self.device)
                
                with torch.no_grad():
                    outputs = self.model.generate(
                        inputs,
                        max_length=inputs.shape[1] + 50,
                        temperature=0.7,
                        do_sample=True,
                        pad_token_id=self.tokenizer.eos_token_id
                    )
                
                generated = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
                generated_texts.append(generated)
            
            batch["sentiment"] = sentiment_results
            batch["generated_text"] = generated_texts
            
            return batch

    # Apply Hugging Face processing
    hf_processed = text_data.map_batches(
        HuggingFaceTextProcessor,
        compute=ray.data.ActorPoolStrategy(size=2),
        num_gpus=1
    )

**Custom LLM Integration**

.. code-block:: python

    import openai
    from typing import Dict, Any
    import ray

    class OpenAIProcessor:
        """Text processing with OpenAI models."""
        
        def __init__(self, api_key: str):
            openai.api_key = api_key
        
        def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
            """Process texts with OpenAI models."""
            
            analysis_results = []
            
            for text in batch["text"]:
                try:
                    # Analyze text with OpenAI
                    response = openai.ChatCompletion.create(
                        model="gpt-3.5-turbo",
                        messages=[
                            {"role": "system", "content": "Analyze the following text and provide key insights."},
                            {"role": "user", "content": text[:1000]}  # Limit length
                        ],
                        max_tokens=200
                    )
                    
                    analysis = response.choices[0].message.content
                    analysis_results.append(analysis)
                    
                except Exception as e:
                    analysis_results.append(f"Error: {str(e)}")
            
            batch["openai_analysis"] = analysis_results
            return batch

    # Apply OpenAI processing
    openai_processed = text_data.map_batches(
        OpenAIProcessor,
        compute=ray.data.ActorPoolStrategy(size=1)  # Limit concurrent API calls
    )

Performance Optimization
------------------------

Optimize text processing pipelines for maximum performance and efficiency.

**Batch Size Optimization**

.. code-block:: python

    from ray.data.context import DataContext
    import ray

    # Configure optimal batch sizes for text processing
    ctx = DataContext.get_current()
    
    # For text processing, moderate batch sizes work well
    ctx.target_max_block_size = 128 * 1024 * 1024  # 128MB blocks
    
    # Optimize batch sizes based on text characteristics
    def optimize_text_batch_size(text_data):
        """Determine optimal batch size for text processing."""
        
        # Analyze text characteristics
        sample_batch = text_data.take_batch(batch_size=100)
        avg_text_length = sum(len(t) for t in sample_batch["text"]) / len(sample_batch["text"])
        
        # Calculate optimal batch size
        target_batch_size = int(64 * 1024 * 1024 / (avg_text_length * 4))  # Estimate 4 bytes per char
        
        # Ensure reasonable bounds
        target_batch_size = max(1, min(target_batch_size, 128))
        
        return target_batch_size

    # Apply optimized batch processing
    optimal_batch_size = optimize_text_batch_size(text_data)
    optimized_pipeline = text_data.map_batches(
        process_text,
        batch_size=optimal_batch_size
    )

**Memory Management**

.. code-block:: python

    def memory_efficient_text_processing(batch: Dict[str, Any]) -> Dict[str, Any]:
        """Process text with memory efficiency."""
        
        # Process in smaller chunks to manage memory
        chunk_size = 64
        results = []
        
        for i in range(0, len(batch["text"]), chunk_size):
            chunk = batch["text"][i:i+chunk_size]
            
            # Process chunk
            processed_chunk = process_text_chunk(chunk)
            results.extend(processed_chunk)
            
            # Explicitly clear chunk from memory
            del chunk
        
        batch["processed_text"] = results
        return batch

    # Use memory-efficient processing
    memory_optimized = text_data.map_batches(memory_efficient_text_processing)

**Caching and Optimization**

.. code-block:: python

    from functools import lru_cache
    import ray

    class OptimizedTextProcessor:
        """Text processor with caching and optimization."""
        
        def __init__(self):
            # Cache for expensive operations
            self.cache = {}
        
        @lru_cache(maxsize=1000)
        def cached_text_analysis(self, text_hash: str):
            """Cache expensive text analysis operations."""
            # Implementation here
            pass
        
        def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
            """Process texts with optimization."""
            
            # Use caching for repeated operations
            # Implement other optimizations
            
            return batch

    # Apply optimized processing
    optimized_processed = text_data.map_batches(OptimizedTextProcessor())

Saving and Exporting Text
--------------------------

Save processed text data in various formats for different use cases.

**Text File Formats**

.. code-block:: python

    # Save as text files
    processed_text.write_text(
        "s3://output/processed-text/",
        column="processed_text"
    )

    # Save with custom naming
    def save_with_metadata(batch):
        """Save text with metadata in filenames."""
        
        for i, (text, metadata) in enumerate(zip(batch["processed_text"], batch["metadata"])):
            filename = f"doc_{metadata['id']}_{metadata['timestamp']}.txt"
            # Save logic here
        
        return batch

    # Save with custom logic
    custom_saved = processed_text.map_batches(save_with_metadata)

**Structured Formats**

.. code-block:: python

    # Save as JSON Lines
    processed_text.write_json(
        "s3://output/processed-text.jsonl",
        column="processed_text"
    )

    # Save as Parquet with metadata
    processed_text.write_parquet(
        "s3://output/text-dataset/",
        compression="snappy"
    )

    # Save as CSV
    processed_text.write_csv(
        "s3://output/text-data.csv"
    )

**Database Export**

.. code-block:: python

    # Save to database
    processed_text.write_database(
        "postgresql://user:pass@host:5432/db",
        "processed_texts",
        mode="overwrite"
    )

    # Save to data warehouse
    processed_text.write_snowflake(
        "snowflake://user:pass@account/database/schema",
        "PROCESSED_TEXTS"
    )

Integration with ML Frameworks
------------------------------

Integrate Ray Data text processing with popular machine learning frameworks.

**PyTorch Integration**

.. code-block:: python

    import torch
    from torch.utils.data import DataLoader
    import ray

    # Convert Ray Dataset to PyTorch format
    torch_dataset = processed_text.to_torch(
        label_column="label",
        feature_columns=["processed_text"],
        batch_size=32
    )

    # Use with PyTorch training
    model = YourPyTorchModel()
    optimizer = torch.optim.Adam(model.parameters())
    
    for batch in torch_dataset:
        texts = batch["processed_text"]
        labels = batch["label"]
        
        # Training step
        optimizer.zero_grad()
        outputs = model(texts)
        loss = torch.nn.functional.cross_entropy(outputs, labels)
        loss.backward()
        optimizer.step()

**TensorFlow Integration**

.. code-block:: python

    import tensorflow as tf
    import ray

    # Convert Ray Dataset to TensorFlow format
    tf_dataset = processed_text.to_tf(
        label_column="label",
        feature_columns=["processed_text"],
        batch_size=32
    )

    # Use with TensorFlow training
    model = tf.keras.Sequential([
        tf.keras.layers.Embedding(10000, 128),
        tf.keras.layers.LSTM(64),
        tf.keras.layers.Dense(10, activation='softmax')
    ])
    
    model.compile(
        optimizer='adam',
        loss='sparse_categorical_crossentropy',
        metrics=['accuracy']
    )
    
    model.fit(tf_dataset, epochs=10)

**Hugging Face Integration**

.. code-block:: python

    from transformers import Trainer, TrainingArguments
    from datasets import Dataset
    import ray

    # Convert Ray Dataset to Hugging Face Dataset
    text_data = ray.data.read_json("s3://text-dataset/")
    pandas_df = text_data.to_pandas()
    hf_dataset = Dataset.from_pandas(pandas_df)

    # Use with Hugging Face Trainer
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

Production Deployment
---------------------

Deploy text processing pipelines to production with monitoring and optimization.

**Production Pipeline Configuration**

.. code-block:: python

    def production_text_pipeline():
        """Production-ready text processing pipeline."""
        
        # Configure for production
        ctx = DataContext.get_current()
        ctx.target_max_block_size = 128 * 1024 * 1024  # 128MB blocks
        ctx.enable_auto_log_stats = True
        ctx.verbose_stats_logs = True
        
        # Load text data
        text_data = ray.data.read_text("s3://input/documents/")
        
        # Apply processing
        processed = text_data.map_batches(
            production_text_processor,
            compute=ray.data.ActorPoolStrategy(size=4),
            batch_size=64
        )
        
        # Save results
        processed.write_parquet("s3://output/processed-text/")
        
        return processed

**Monitoring and Observability**

.. code-block:: python

    # Enable comprehensive monitoring
    ctx = DataContext.get_current()
    ctx.enable_per_node_metrics = True
    ctx.memory_usage_poll_interval_s = 1.0

    # Monitor pipeline performance
    def monitor_pipeline_performance(dataset):
        """Monitor text processing pipeline performance."""
        
        stats = dataset.stats()
        print(f"Processing time: {stats.total_time}")
        print(f"Memory usage: {stats.memory_usage}")
        print(f"CPU usage: {stats.cpu_usage}")
        
        return dataset

    # Apply monitoring
    monitored_pipeline = text_data.map_batches(
        process_text
    ).map_batches(monitor_pipeline_performance)

Best Practices
--------------

**1. Text Preprocessing**

* Clean and normalize text consistently
* Handle encoding issues properly
* Implement robust error handling for malformed text

**2. Batch Size Optimization**

* Start with moderate batch sizes (32-64)
* Adjust based on text length and complexity
* Monitor memory usage and adjust accordingly

**3. Memory Management**

* Use streaming execution for large datasets
* Process text in chunks to manage memory
* Clear intermediate results when possible

**4. Model Integration**

* Use appropriate model sizes for your use case
* Implement proper error handling for model failures
* Monitor model performance and resource usage

**5. Error Handling**

* Implement robust error handling for text processing
* Use `max_errored_blocks` to handle failures gracefully
* Log and monitor processing errors

Next Steps
----------

Now that you understand text processing with Ray Data, explore related topics:

* **Working with AI**: AI and machine learning workflows → :ref:`working-with-ai`
* **Working with LLMs**: Large language model workflows → :ref:`working-with-llms`
* **Working with PyTorch**: Deep PyTorch integration → :ref:`working-with-pytorch`
* **Performance Optimization**: Optimize text processing performance → :ref:`performance-optimization`

For practical examples:

* **NLP Examples**: Real-world natural language processing applications → :ref:`nlp-examples`
* **Text Analysis Examples**: Text transformation and analysis → :ref:`text-analysis-examples`
* **LLM Integration Examples**: Integration with language models → :ref:`llm-integration-examples`
