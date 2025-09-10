.. _content-moderation:

Content Moderation: AI-Powered Safety & Compliance Pipeline
===========================================================

**Keywords:** content moderation, AI safety, content filtering, image moderation, text moderation, compliance automation, safety pipeline, content classification, automated moderation

**Navigation:** :ref:`Ray Data <data>` → :ref:`Use Cases <use_cases>` → Content Moderation

This use case demonstrates building AI-powered content moderation pipelines that automatically detect inappropriate content, ensure compliance, and maintain platform safety at scale using Ray Data's multimodal processing capabilities.

**What you'll build:**

* Multi-modal content safety pipeline (text, images, video)
* Automated compliance checking and flagging
* Content classification and risk scoring
* Human review queue optimization

Content Moderation Architecture
-------------------------------

**Ray Data's Content Moderation Advantages**

:::list-table
   :header-rows: 1

- - **Content Type**
  - **Moderation Capability**
  - **AI Models**
  - **Processing Scale**
- - Text Content
  - Toxicity, hate speech, spam detection
  - BERT, RoBERTa, custom classifiers
  - Millions of posts per hour
- - Images
  - NSFW, violence, inappropriate content
  - Vision transformers, CNNs
  - Thousands of images per minute
- - Video
  - Frame-level analysis, audio transcription
  - Multi-modal models, speech-to-text
  - Hours of video content per batch
- - User Behavior
  - Pattern analysis, anomaly detection
  - Clustering, outlier detection
  - Real-time behavioral scoring

:::

Use Case: Multi-Modal Content Safety Pipeline
----------------------------------------------

**Business Scenario:** Build a comprehensive content moderation system for a social platform that processes text posts, images, and videos for safety and compliance.

.. code-block:: python

    import ray
    import pandas as pd
    import numpy as np
    from transformers import pipeline

    def content_moderation_pipeline():
        """Moderate content across multiple modalities for platform safety."""
        
        # Load user-generated content
        text_posts = ray.data.read_json("s3://user-content/text-posts/")
        user_images = ray.data.read_images("s3://user-content/images/")
        video_content = ray.data.read_videos("s3://user-content/videos/")
        
        def moderate_text_content(batch):
            """Analyze text content for safety and compliance."""
            # Initialize moderation models
            toxicity_classifier = pipeline(
                "text-classification",
                model="unitary/toxic-bert"
            )
            
            hate_speech_classifier = pipeline(
                "text-classification", 
                model="cardiffnlp/twitter-roberta-base-hate-latest"
            )
            
            moderation_results = []
            
            for item in batch.to_pylist():
                text = item.get("text", item.get("content", ""))
                post_id = item.get("post_id", "unknown")
                user_id = item.get("user_id", "unknown")
                
                # Skip very short content
                if len(text.strip()) < 5:
                    continue
                
                # Toxicity detection
                toxicity_result = toxicity_classifier(text[:512])
                is_toxic = toxicity_result[0]["label"] == "TOXIC"
                toxicity_confidence = toxicity_result[0]["score"]
                
                # Hate speech detection
                hate_result = hate_speech_classifier(text[:512])
                is_hate_speech = hate_result[0]["label"] == "HATE"
                hate_confidence = hate_result[0]["score"]
                
                # Content analysis
                word_count = len(text.split())
                caps_ratio = sum(1 for c in text if c.isupper()) / max(len(text), 1)
                exclamation_count = text.count("!")
                
                # Risk scoring
                risk_factors = [
                    is_toxic and toxicity_confidence > 0.7,
                    is_hate_speech and hate_confidence > 0.7,
                    caps_ratio > 0.5,  # Excessive caps
                    exclamation_count > 5  # Excessive exclamations
                ]
                
                risk_score = sum(risk_factors) / len(risk_factors)
                
                # Moderation decision
                if risk_score > 0.5:
                    action = "block"
                elif risk_score > 0.25:
                    action = "review"
                else:
                    action = "approve"
                
                moderation_results.append({
                    "post_id": post_id,
                    "user_id": user_id,
                    "content_type": "text",
                    "text": text,
                    "is_toxic": is_toxic,
                    "toxicity_confidence": toxicity_confidence,
                    "is_hate_speech": is_hate_speech,
                    "hate_confidence": hate_confidence,
                    "caps_ratio": caps_ratio,
                    "risk_score": risk_score,
                    "moderation_action": action,
                    "requires_human_review": action == "review",
                    "moderated_at": pd.Timestamp.now()
                })
            
            return ray.data.from_pylist(moderation_results)
        
        def moderate_image_content(batch):
            """Analyze image content for safety compliance."""
            # Load image safety models (cached in actor)
            # nsfw_model = load_nsfw_detection_model()
            # violence_model = load_violence_detection_model()
            
            moderation_results = []
            
            for item in batch.to_pylist():
                image = item["image"]
                path = item["path"]
                
                # Extract image metadata
                post_id = path.split("/")[-1].split(".")[0]
                height, width, channels = image.shape
                
                # Simulate safety model inference
                # nsfw_score = nsfw_model.predict(image)
                # violence_score = violence_model.predict(image)
                
                # Simulate moderation scores
                nsfw_score = np.random.beta(1, 9)  # Low probability of NSFW
                violence_score = np.random.beta(1, 19)  # Very low probability of violence
                
                # Image quality assessment
                brightness = np.mean(image)
                contrast = np.std(image)
                quality_score = min(brightness / 255 * contrast / 128, 1.0)
                
                # Risk assessment
                safety_risk = max(nsfw_score, violence_score)
                
                # Moderation decision
                if safety_risk > 0.8:
                    action = "block"
                elif safety_risk > 0.5 or quality_score < 0.3:
                    action = "review"
                else:
                    action = "approve"
                
                moderation_results.append({
                    "post_id": post_id,
                    "content_type": "image",
                    "image_width": width,
                    "image_height": height,
                    "nsfw_score": nsfw_score,
                    "violence_score": violence_score,
                    "quality_score": quality_score,
                    "safety_risk": safety_risk,
                    "moderation_action": action,
                    "requires_human_review": action == "review",
                    "moderated_at": pd.Timestamp.now()
                })
            
            return ray.data.from_pylist(moderation_results)
        
        def create_moderation_summary(batch):
            """Create summary for moderation team."""
            # Add priority levels
            def determine_priority(row):
                if row["moderation_action"] == "block":
                    return "urgent"
                elif row["requires_human_review"]:
                    return "high"
                else:
                    return "low"
            
            batch["review_priority"] = batch.apply(determine_priority, axis=1)
            
            # Add estimated review time
            batch["estimated_review_minutes"] = batch.apply(
                lambda row: 5 if row["content_type"] == "video" else
                           3 if row["content_type"] == "image" else 1, axis=1
            )
            
            return batch
        
        # Process text content with GPU acceleration for models
        moderated_text = text_posts.map_batches(
            moderate_text_content,
            compute=ray.data.ActorPoolStrategy(size=4),
            num_gpus=0.5  # Share GPU for NLP models
        )
        
        # Process image content with GPU acceleration
        moderated_images = user_images.map_batches(
            moderate_image_content,
            compute=ray.data.ActorPoolStrategy(size=4),
            num_gpus=1  # Dedicated GPU for image processing
        )
        
        # Combine moderation results
        all_moderated_content = moderated_text.union(moderated_images)
        
        # Create moderation summary
        moderation_summary = all_moderated_content.map_batches(create_moderation_summary)
        
        # Separate content by moderation decision
        approved_content = moderation_summary.filter(
            lambda row: row["moderation_action"] == "approve"
        )
        
        review_queue = moderation_summary.filter(
            lambda row: row["requires_human_review"]
        )
        
        blocked_content = moderation_summary.filter(
            lambda row: row["moderation_action"] == "block"
        )
        
        # Save moderation results
        approved_content.write_parquet("s3://moderated-content/approved/")
        review_queue.write_parquet("s3://moderated-content/review-queue/")
        blocked_content.write_parquet("s3://moderated-content/blocked/")
        
        # Create moderation analytics
        moderation_stats = moderation_summary.groupby(["content_type", "moderation_action"]).aggregate(
            ray.data.aggregate.Count("post_id"),
            ray.data.aggregate.Mean("safety_risk") if "safety_risk" in moderation_summary.schema().names else ray.data.aggregate.Mean("risk_score")
        )
        
        moderation_stats.write_csv("s3://reports/moderation-summary.csv")
        
        return approved_content, review_queue, blocked_content, moderation_stats

**Content Moderation Pipeline Checklist**

**Safety Model Integration:**
- [ ] **Model accuracy**: Use well-validated safety and toxicity detection models
- [ ] **Multi-language support**: Handle content in multiple languages appropriately
- [ ] **Model updates**: Plan for regular model updates and improvements
- [ ] **False positive handling**: Implement appeal and review processes
- [ ] **Performance monitoring**: Track model accuracy and processing speed

**Compliance and Legal:**
- [ ] **Regulatory compliance**: Ensure compliance with platform policies and laws
- [ ] **Data retention**: Implement appropriate data retention policies
- [ ] **Audit logging**: Maintain comprehensive audit trails for decisions
- [ ] **Privacy protection**: Handle user data according to privacy requirements
- [ ] **Transparency**: Provide clear explanation for moderation decisions

**Operational Excellence:**
- [ ] **Human review integration**: Efficient workflows for human moderators
- [ ] **Priority queuing**: Route urgent content for immediate review
- [ ] **Performance scaling**: Handle traffic spikes and high volume periods
- [ ] **Quality assurance**: Regular quality checks on moderation decisions
- [ ] **Feedback loops**: Incorporate human reviewer feedback to improve models

Next Steps
----------

Enhance your content moderation capabilities:

* **Advanced AI Models**: Implement specialized safety models → :ref:`AI-Powered Pipelines <ai-powered-pipelines>`
* **Computer Vision Safety**: Advanced image/video moderation → :ref:`Working with Images <working_with_images>`
* **NLP Safety**: Text analysis and classification → :ref:`NLP Data Processing <nlp-data-processing>`
* **Production Deployment**: Scale moderation systems → :ref:`Best Practices <best_practices>`
