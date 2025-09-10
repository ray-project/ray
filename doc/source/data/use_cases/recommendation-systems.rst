.. _recommendation-systems:

Recommendation Systems: Personalized AI-Powered Recommendations
===============================================================

**Keywords:** recommendation systems, collaborative filtering, content-based filtering, personalization, recommender models, user behavior analysis, item similarity, machine learning recommendations

**Navigation:** :ref:`Ray Data <data>` → :ref:`Use Cases <use_cases>` → Recommendation Systems

This use case demonstrates building scalable recommendation systems using Ray Data's multimodal processing capabilities to combine user behavior, content features, and collaborative signals for personalized recommendations.

**What you'll build:**

* User behavior analysis and preference extraction
* Content-based recommendation feature pipeline
* Collaborative filtering data preparation
* Hybrid recommendation model training data

Recommendation System Architecture
----------------------------------

**Ray Data's Recommendation Advantages**

:::list-table
   :header-rows: 1

- - **Recommendation Type**
  - **Data Requirements**
  - **Ray Data Capability**
  - **Scaling Benefits**
- - Content-Based
  - Item features, user profiles
  - Multimodal feature extraction
  - Process millions of items with complex features
- - Collaborative Filtering
  - User-item interactions
  - Distributed matrix operations
  - Handle billions of interactions efficiently
- - Hybrid Systems
  - Combined signals and features
  - Unified multimodal processing
  - Integrate all recommendation signals
- - Deep Learning
  - Rich feature representations
  - GPU-accelerated preprocessing
  - Prepare complex training data at scale

:::

Use Case: E-commerce Recommendation Pipeline
---------------------------------------------

**Business Scenario:** Build a comprehensive recommendation system for an e-commerce platform that combines user behavior, product features, and collaborative signals.

.. code-block:: python

    import ray
    import pandas as pd
    import numpy as np
    from sklearn.metrics.pairwise import cosine_similarity

    def ecommerce_recommendation_pipeline():
        """Build recommendation system for e-commerce platform."""
        
        # Load user interaction data
        user_interactions = ray.data.read_parquet("s3://user-data/interactions/")
        product_catalog = ray.data.read_parquet("s3://products/catalog/")
        user_profiles = ray.data.read_parquet("s3://users/profiles/")
        
        def create_user_preference_features(batch):
            """Extract user preferences from interaction history."""
            user_features = []
            
            for user_id, user_data in batch.groupby("user_id"):
                # Interaction statistics
                total_interactions = len(user_data)
                unique_products = user_data["product_id"].nunique()
                avg_rating = user_data["rating"].mean() if "rating" in user_data.columns else 3.5
                
                # Category preferences
                category_interactions = user_data["category"].value_counts()
                top_categories = category_interactions.head(3).index.tolist()
                category_diversity = len(category_interactions) / max(unique_products, 1)
                
                # Price sensitivity analysis
                if "price" in user_data.columns:
                    avg_price_preference = user_data["price"].mean()
                    price_range_preference = user_data["price"].std()
                else:
                    avg_price_preference = 50.0  # Default
                    price_range_preference = 25.0
                
                # Interaction patterns
                interaction_types = user_data["interaction_type"].value_counts() if "interaction_type" in user_data.columns else {}
                view_to_purchase_ratio = interaction_types.get("purchase", 0) / max(interaction_types.get("view", 1), 1)
                
                # Temporal preferences
                user_data["hour"] = pd.to_datetime(user_data["timestamp"]).dt.hour
                preferred_hours = user_data["hour"].mode().tolist()
                
                user_features.append({
                    "user_id": user_id,
                    "total_interactions": total_interactions,
                    "unique_products_interacted": unique_products,
                    "avg_rating": avg_rating,
                    "top_category_1": top_categories[0] if len(top_categories) > 0 else "unknown",
                    "top_category_2": top_categories[1] if len(top_categories) > 1 else "unknown", 
                    "top_category_3": top_categories[2] if len(top_categories) > 2 else "unknown",
                    "category_diversity": category_diversity,
                    "avg_price_preference": avg_price_preference,
                    "price_range_preference": price_range_preference,
                    "view_to_purchase_ratio": view_to_purchase_ratio,
                    "preferred_shopping_hours": preferred_hours[:3],  # Top 3 hours
                    "user_engagement_score": min(total_interactions / 100, 1.0),
                    "preference_strength": avg_rating * category_diversity
                })
            
            return ray.data.from_pylist(user_features)
        
        def create_product_similarity_features(batch):
            """Create product similarity and content features."""
            product_features = []
            
            for _, product in batch.iterrows():
                product_id = product["product_id"]
                
                # Content features
                category = product.get("category", "unknown")
                brand = product.get("brand", "unknown")
                price = product.get("price", 0)
                
                # Create feature vector for similarity
                # In production, this would include rich product features
                feature_vector = [
                    hash(category) % 1000,  # Category encoding
                    hash(brand) % 500,      # Brand encoding
                    price,                  # Price feature
                    product.get("rating", 3.5),  # Average rating
                    product.get("review_count", 0)  # Social proof
                ]
                
                # Normalize feature vector
                feature_vector = np.array(feature_vector)
                feature_vector = feature_vector / (np.linalg.norm(feature_vector) + 1e-8)
                
                product_features.append({
                    "product_id": product_id,
                    "category": category,
                    "brand": brand,
                    "price": price,
                    "feature_vector": feature_vector.tolist(),
                    "content_features_ready": True
                })
            
            return ray.data.from_pylist(product_features)
        
        def create_collaborative_signals(batch):
            """Create collaborative filtering signals."""
            cf_signals = []
            
            # Create user-item interaction matrix data
            for _, interaction in batch.iterrows():
                user_id = interaction["user_id"]
                product_id = interaction["product_id"]
                rating = interaction.get("rating", 1)  # Implicit feedback
                timestamp = interaction["timestamp"]
                
                # Weight recent interactions more heavily
                days_ago = (pd.Timestamp.now() - pd.to_datetime(timestamp)).days
                recency_weight = max(0.1, 1.0 - days_ago / 365)  # Decay over year
                
                # Interaction strength
                interaction_strength = rating * recency_weight
                
                cf_signals.append({
                    "user_id": user_id,
                    "product_id": product_id,
                    "interaction_strength": interaction_strength,
                    "recency_weight": recency_weight,
                    "original_rating": rating,
                    "days_since_interaction": days_ago
                })
            
            return ray.data.from_pylist(cf_signals)
        
        # Extract user preferences
        user_preferences = user_interactions.map_batches(
            create_user_preference_features,
            compute=ray.data.ActorPoolStrategy(size=6)
        )
        
        # Create product content features
        product_features = product_catalog.map_batches(
            create_product_similarity_features,
            compute=ray.data.ActorPoolStrategy(size=4)
        )
        
        # Create collaborative filtering signals
        collaborative_signals = user_interactions.map_batches(
            create_collaborative_signals,
            compute=ray.data.ActorPoolStrategy(size=4)
        )
        
        # Create recommendation training data
        def prepare_recommendation_training_data(user_prefs, prod_features, cf_signals):
            """Combine all signals for recommendation model training."""
            
            # Join user preferences with collaborative signals
            user_item_features = cf_signals.join(user_prefs, on="user_id", how="inner")
            
            # Join with product features
            complete_features = user_item_features.join(prod_features, on="product_id", how="inner")
            
            return complete_features
        
        # Combine all recommendation signals
        recommendation_training_data = prepare_recommendation_training_data(
            user_preferences, product_features, collaborative_signals
        )
        
        # Create positive and negative samples for training
        def create_training_samples(batch):
            """Create positive and negative samples for model training."""
            training_samples = []
            
            for _, row in batch.iterrows():
                user_id = row["user_id"]
                product_id = row["product_id"]
                
                # Positive sample (actual interaction)
                positive_sample = {
                    **row.to_dict(),
                    "label": 1,
                    "sample_type": "positive"
                }
                training_samples.append(positive_sample)
                
                # Create negative sample (random product user didn't interact with)
                # In production, use more sophisticated negative sampling
                negative_product_id = f"neg_{hash(f'{user_id}_{product_id}') % 10000}"
                negative_sample = {
                    **row.to_dict(),
                    "product_id": negative_product_id,
                    "interaction_strength": 0,
                    "label": 0,
                    "sample_type": "negative"
                }
                training_samples.append(negative_sample)
            
            return ray.data.from_pylist(training_samples)
        
        # Create training samples
        training_samples = recommendation_training_data.map_batches(create_training_samples)
        
        # Save recommendation training data
        training_samples.write_parquet("s3://recommendation-training/samples/")
        
        # Create recommendation model evaluation data
        evaluation_summary = training_samples.groupby(["sample_type", "top_category_1"]).aggregate(
            ray.data.aggregate.Count("user_id"),
            ray.data.aggregate.Mean("interaction_strength")
        )
        
        evaluation_summary.write_csv("s3://reports/recommendation-training-summary.csv")
        
        return training_samples, evaluation_summary

**Recommendation System Checklist**

**Data Preparation:**
- [ ] **User behavior tracking**: Capture comprehensive interaction data
- [ ] **Content features**: Extract rich product/item features
- [ ] **Temporal signals**: Include time-based interaction patterns
- [ ] **Negative sampling**: Create appropriate negative training samples
- [ ] **Data freshness**: Ensure recent user behavior is captured

**Feature Engineering:**
- [ ] **User embeddings**: Create user representation vectors
- [ ] **Item embeddings**: Generate item feature representations
- [ ] **Interaction features**: Capture interaction context and strength
- [ ] **Collaborative signals**: Extract user-user and item-item similarities
- [ ] **Content features**: Include rich content-based features

**Model Training Preparation:**
- [ ] **Training data balance**: Ensure appropriate positive/negative sample ratios
- [ ] **Feature scaling**: Normalize features for model training
- [ ] **Data splits**: Create proper train/validation/test splits
- [ ] **Cold start handling**: Prepare features for new users/items
- [ ] **Evaluation metrics**: Define appropriate recommendation quality metrics

Next Steps
----------

Enhance your recommendation systems:

* **Advanced Feature Engineering**: Complex user and item features → :ref:`Feature Engineering <feature-engineering>`
* **Model Training**: Prepare training pipelines → :ref:`Model Training Pipelines <model-training-pipelines>`
* **Multimodal Recommendations**: Include visual and text features → :ref:`AI-Powered Pipelines <ai-powered-pipelines>`
* **Production Deployment**: Scale recommendation systems → :ref:`Best Practices <best_practices>`
