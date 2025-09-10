.. _nlp-data-processing:

NLP Data Processing: Large-Scale Text Analysis & Language Models
================================================================

**Keywords:** natural language processing, NLP, text analysis, sentiment analysis, language models, text classification, named entity recognition, text preprocessing, HuggingFace, transformers, BERT, GPT

**Navigation:** :ref:`Ray Data <data>` → :ref:`Use Cases <use_cases>` → NLP Data Processing

This use case demonstrates Ray Data's capabilities for large-scale natural language processing, including text preprocessing, sentiment analysis, entity extraction, and language model fine-tuning data preparation.

**What you'll build:**

* Large-scale text preprocessing pipeline for language models
* Sentiment analysis and classification workflow
* Named entity recognition and extraction pipeline
* Text quality assessment and content moderation

NLP Pipeline Capabilities
-------------------------

**Ray Data's NLP Processing Advantages**

:::list-table
   :header-rows: 1

- - **NLP Task**
  - **Ray Data Capability**
  - **Scaling Benefits**
  - **Integration Options**
- - Text Preprocessing
  - Distributed tokenization and cleaning
  - Process millions of documents
  - HuggingFace Tokenizers, spaCy
- - Sentiment Analysis
  - Batch model inference with GPU
  - High-throughput classification
  - Transformers, custom models
- - Entity Recognition
  - Parallel NER processing
  - Extract entities from large corpora
  - spaCy, HuggingFace NER models
- - Language Model Training
  - Distributed data preparation
  - Prepare massive training datasets
  - PyTorch, TensorFlow, JAX
- - Text Classification
  - Multi-label batch classification
  - Classify documents at scale
  - BERT, RoBERTa, custom models

:::

Use Case 1: Large-Scale Text Preprocessing Pipeline
----------------------------------------------------

**Business Scenario:** Prepare large text corpora for language model training or fine-tuning, including cleaning, tokenization, and quality filtering.

.. code-block:: python

    import ray
    import pandas as pd
    import re
    from transformers import AutoTokenizer
    import numpy as np

    def large_scale_text_preprocessing():
        """Preprocess large text corpora for language model training."""
        
        # Load text data from various sources
        documents = ray.data.read_text("s3://text-corpus/documents/")
        web_content = ray.data.read_json("s3://text-corpus/web-scraped/")
        
        def clean_and_filter_text(batch):
            """Clean and filter text for quality."""
            cleaned_texts = []
            
            for item in batch.to_pylist():
                if isinstance(item, dict):
                    text = item.get("text", item.get("content", ""))
                    source = item.get("source", "unknown")
                else:
                    text = str(item)
                    source = "file"
                
                # Text cleaning
                # Remove excessive whitespace
                cleaned_text = re.sub(r'\s+', ' ', text).strip()
                
                # Remove special characters but preserve punctuation
                cleaned_text = re.sub(r'[^\w\s.,!?;:\-\'"()]', '', cleaned_text)
                
                # Filter by length and quality
                word_count = len(cleaned_text.split())
                char_count = len(cleaned_text)
                
                # Quality metrics
                avg_word_length = char_count / max(word_count, 1)
                punctuation_ratio = sum(1 for c in cleaned_text if c in '.,!?;:') / max(char_count, 1)
                
                # Quality filtering criteria
                is_high_quality = (
                    word_count >= 10 and  # Minimum length
                    word_count <= 5000 and  # Maximum length
                    avg_word_length >= 3 and  # Reasonable word length
                    avg_word_length <= 15 and  # Not too long words
                    punctuation_ratio <= 0.1 and  # Not excessive punctuation
                    not re.search(r'(.)\1{4,}', cleaned_text)  # No repeated characters
                )
                
                if is_high_quality:
                    cleaned_texts.append({
                        "text": cleaned_text,
                        "word_count": word_count,
                        "char_count": char_count,
                        "avg_word_length": avg_word_length,
                        "punctuation_ratio": punctuation_ratio,
                        "source": source,
                        "quality_score": 1.0 - abs(avg_word_length - 5) / 10  # Peak at 5 chars/word
                    })
            
            return ray.data.from_pylist(cleaned_texts)
        
        def tokenize_for_language_model(batch):
            """Tokenize text for language model training."""
            # Initialize tokenizer
            tokenizer = AutoTokenizer.from_pretrained("bert-base-uncased")
            
            tokenized_data = []
            for item in batch.to_pylist():
                text = item["text"]
                
                # Tokenize text
                tokens = tokenizer(
                    text,
                    truncation=True,
                    padding=False,
                    max_length=512,
                    return_tensors="np"
                )
                
                # Extract token information
                input_ids = tokens["input_ids"][0].tolist()
                attention_mask = tokens["attention_mask"][0].tolist()
                
                tokenized_data.append({
                    "text": text,
                    "input_ids": input_ids,
                    "attention_mask": attention_mask,
                    "token_count": len(input_ids),
                    "effective_length": sum(attention_mask),
                    "source": item["source"],
                    "quality_score": item["quality_score"]
                })
            
            return ray.data.from_pylist(tokenized_data)
        
        # Clean and filter text data
        cleaned_documents = documents.map_batches(clean_and_filter_text)
        cleaned_web_content = web_content.map_batches(clean_and_filter_text)
        
        # Combine all text sources
        all_text = cleaned_documents.union(cleaned_web_content)
        
        # Tokenize for language model training
        tokenized_text = all_text.map_batches(
            tokenize_for_language_model,
            compute=ray.data.ActorPoolStrategy(size=8)  # CPU-intensive tokenization
        )
        
        # Filter by quality and token length
        high_quality_tokens = tokenized_text.filter(
            lambda row: row["quality_score"] > 0.7 and 
                       row["token_count"] >= 50 and 
                       row["token_count"] <= 512
        )
        
        # Save preprocessed data for training
        high_quality_tokens.write_parquet("s3://preprocessed-text/training-ready/")
        
        # Create quality summary
        quality_summary = tokenized_text.groupby("source").aggregate(
            ray.data.aggregate.Count("text"),
            ray.data.aggregate.Mean("quality_score"),
            ray.data.aggregate.Mean("token_count")
        )
        
        quality_summary.write_csv("s3://reports/text-quality-summary.csv")
        
        return high_quality_tokens, quality_summary

Use Case 2: Sentiment Analysis Pipeline
----------------------------------------

**Business Scenario:** Analyze customer feedback, reviews, and social media content for sentiment and topic extraction at scale.

.. code-block:: python

    import ray
    from transformers import pipeline
    import pandas as pd

    def sentiment_analysis_pipeline():
        """Analyze sentiment in customer feedback at scale."""
        
        # Load customer feedback from multiple sources
        reviews = ray.data.read_json("s3://feedback/product-reviews/")
        support_tickets = ray.data.read_json("s3://feedback/support-tickets/")
        social_mentions = ray.data.read_json("s3://feedback/social-media/")
        
        def analyze_sentiment_and_topics(batch):
            """Analyze sentiment and extract topics from text."""
            # Initialize NLP models
            sentiment_analyzer = pipeline(
                "sentiment-analysis",
                model="cardiffnlp/twitter-roberta-base-sentiment-latest"
            )
            
            emotion_analyzer = pipeline(
                "text-classification",
                model="j-hartmann/emotion-english-distilroberta-base"
            )
            
            results = []
            for item in batch.to_pylist():
                text = item.get("text", item.get("message", item.get("content", "")))
                customer_id = item.get("customer_id", "unknown")
                timestamp = item.get("timestamp", pd.Timestamp.now())
                source = item.get("source", "unknown")
                
                # Limit text length for model processing
                text_sample = text[:512]
                
                if len(text_sample.strip()) < 10:  # Skip very short texts
                    continue
                
                # Sentiment analysis
                sentiment_result = sentiment_analyzer(text_sample)
                sentiment_label = sentiment_result[0]["label"]
                sentiment_score = sentiment_result[0]["score"]
                
                # Emotion analysis
                emotion_result = emotion_analyzer(text_sample)
                primary_emotion = emotion_result[0]["label"]
                emotion_score = emotion_result[0]["score"]
                
                # Text characteristics
                word_count = len(text.split())
                exclamation_count = text.count("!")
                question_count = text.count("?")
                caps_ratio = sum(1 for c in text if c.isupper()) / max(len(text), 1)
                
                # Urgency and priority scoring
                urgency_indicators = ["urgent", "asap", "immediately", "critical", "emergency"]
                urgency_score = sum(1 for indicator in urgency_indicators 
                                  if indicator.lower() in text.lower()) / len(urgency_indicators)
                
                results.append({
                    "customer_id": customer_id,
                    "text": text,
                    "text_length": len(text),
                    "word_count": word_count,
                    "sentiment_label": sentiment_label,
                    "sentiment_score": sentiment_score,
                    "primary_emotion": primary_emotion,
                    "emotion_score": emotion_score,
                    "urgency_score": urgency_score,
                    "caps_ratio": caps_ratio,
                    "exclamation_count": exclamation_count,
                    "question_count": question_count,
                    "source": source,
                    "timestamp": timestamp,
                    "processing_date": pd.Timestamp.now()
                })
            
            return ray.data.from_pylist(results)
        
        def extract_business_insights(batch):
            """Extract business-relevant insights from sentiment analysis."""
            # Classify feedback priority
            def determine_priority(row):
                if row["sentiment_label"] == "NEGATIVE" and row["urgency_score"] > 0.5:
                    return "high_priority"
                elif row["sentiment_label"] == "NEGATIVE":
                    return "medium_priority"
                elif row["urgency_score"] > 0.3:
                    return "medium_priority"
                else:
                    return "low_priority"
            
            batch["business_priority"] = batch.apply(determine_priority, axis=1)
            
            # Create actionable categories
            batch["requires_immediate_action"] = (
                (batch["sentiment_label"] == "NEGATIVE") & 
                (batch["sentiment_score"] > 0.8) & 
                (batch["urgency_score"] > 0.3)
            )
            
            batch["customer_satisfaction_indicator"] = pd.cut(
                batch["sentiment_score"] * (1 if batch["sentiment_label"] == "POSITIVE" else -1),
                bins=[-1, -0.5, 0, 0.5, 1],
                labels=["very_dissatisfied", "dissatisfied", "neutral", "satisfied", "very_satisfied"]
            )
            
            return batch
        
        # Process all feedback sources with GPU acceleration for models
        analyzed_reviews = reviews.map_batches(
            analyze_sentiment_and_topics,
            compute=ray.data.ActorPoolStrategy(size=4),
            num_gpus=0.5  # Share GPU for NLP models
        )
        
        analyzed_tickets = support_tickets.map_batches(
            analyze_sentiment_and_topics,
            compute=ray.data.ActorPoolStrategy(size=4),
            num_gpus=0.5
        )
        
        analyzed_social = social_mentions.map_batches(
            analyze_sentiment_and_topics,
            compute=ray.data.ActorPoolStrategy(size=4),
            num_gpus=0.5
        )
        
        # Combine all analyzed feedback
        all_feedback = analyzed_reviews.union(analyzed_tickets).union(analyzed_social)
        
        # Extract business insights
        business_insights = all_feedback.map_batches(extract_business_insights)
        
        # Create executive summary
        sentiment_summary = business_insights.groupby(["source", "sentiment_label"]).aggregate(
            ray.data.aggregate.Count("customer_id"),
            ray.data.aggregate.Mean("sentiment_score"),
            ray.data.aggregate.Mean("urgency_score")
        )
        
        priority_summary = business_insights.groupby("business_priority").aggregate(
            ray.data.aggregate.Count("customer_id"),
            ray.data.aggregate.Mean("sentiment_score")
        )
        
        # Save NLP analysis results
        business_insights.write_parquet("s3://nlp-analysis/sentiment-analysis/")
        sentiment_summary.write_csv("s3://reports/sentiment-summary.csv")
        priority_summary.write_csv("s3://reports/priority-summary.csv")
        
        return business_insights, sentiment_summary, priority_summary

**NLP Processing Performance Checklist**

**Text Processing Optimization:**
- [ ] **Batch sizing**: Use 100-500 texts per batch for optimal model performance
- [ ] **Text length**: Truncate or chunk long texts appropriately for models
- [ ] **Memory management**: Monitor memory usage with large text datasets
- [ ] **Model caching**: Cache NLP models in actors for efficiency
- [ ] **GPU allocation**: Use GPU resources for transformer model inference

**Model Integration:**
- [ ] **Model selection**: Choose appropriate models for your text domain and language
- [ ] **Inference optimization**: Use batch inference for better GPU utilization
- [ ] **Error handling**: Handle text encoding and model inference errors
- [ ] **Quality validation**: Validate model outputs for consistency
- [ ] **Performance monitoring**: Track inference speed and accuracy

**Pipeline Architecture:**
- [ ] **Streaming execution**: Use streaming for large text corpora
- [ ] **Language detection**: Handle multi-language content appropriately
- [ ] **Encoding handling**: Manage text encoding issues gracefully
- [ ] **Result aggregation**: Efficiently aggregate NLP results
- [ ] **Business integration**: Connect NLP insights to business processes

Next Steps
----------

Expand your NLP capabilities with Ray Data:

* **Advanced Text Analysis**: Topic modeling and document clustering → :ref:`Advanced Analytics <advanced-analytics>`
* **Language Model Training**: Prepare training data → :ref:`Model Training Pipelines <model-training-pipelines>`
* **Multimodal NLP**: Combine text with other data types → :ref:`AI-Powered Pipelines <ai-powered-pipelines>`
* **Production Deployment**: Scale NLP pipelines → :ref:`Best Practices <best_practices>`
