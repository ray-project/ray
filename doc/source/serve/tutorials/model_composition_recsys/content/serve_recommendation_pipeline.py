# serve_recommendation_pipeline.py
import asyncio
import numpy as np
from typing import List, Dict
from ray import serve
from ray.serve.handle import DeploymentHandle
from starlette.requests import Request


# Component 1: User Feature Extractor
@serve.deployment(num_replicas=2)
class UserFeatureExtractor:
    """Extracts user features from user ID.
    
    In production, this queries a database or feature store.
    For this example, the code generates mock features.
    """
    
    async def extract_features(self, user_id: str) -> Dict[str, float]:
        """Extract user features."""
        # Simulate database lookup
        await asyncio.sleep(0.01)
        
        # In production:
        # features = await db.query("SELECT * FROM user_features WHERE user_id = ?", user_id)
        # return features
        
        # Mock features based on user_id hash
        np.random.seed(hash(user_id) % 10000)
        return {
            "age_group": float(np.random.randint(18, 65)),
            "avg_session_duration": float(np.random.uniform(5, 60)),
            "total_purchases": float(np.random.randint(0, 100)),
            "engagement_score": float(np.random.uniform(0, 1)),
        }


# Component 2: Item Ranking Model
@serve.deployment(
    autoscaling_config={
        "min_replicas": 1,
        "max_replicas": 5,
        "target_ongoing_requests": 3
    },
    ray_actor_options={"num_cpus": 2}
)
class ItemRankingModel:
    """Ranks items for a user based on features.
    
    In production, this runs a trained ML model (XGBoost, neural network, etc.).
    For this example, the code uses a simple scoring function.
    """
    
    # Mock item catalog. In production, this comes from a database query.
    CANDIDATE_ITEMS = [f"item_{i}" for i in range(1000)]
    
    def __init__(self):
        # In production, this is your cloud storage path or model registry
        # self.model = load_model("/models/ranking_model.pkl")
        pass
    
    def _score_items(self, user_features: Dict[str, float]) -> List[Dict[str, any]]:
        """Score and rank items for a single user."""
        ranked_items = []
        for item_id in self.CANDIDATE_ITEMS:
            item_popularity = (hash(item_id) % 100) / 100.0
            score = (
                user_features["engagement_score"] * 0.6 + 
                item_popularity * 0.4
            )
            ranked_items.append({
                "item_id": item_id,
                "score": round(score, 3)
            })
        ranked_items.sort(key=lambda x: x["score"], reverse=True)
        return ranked_items
    
    @serve.batch(max_batch_size=32, batch_wait_timeout_s=0.01)
    async def rank_items(
        self, 
        user_features_batch: List[Dict[str, float]]
    ) -> List[List[Dict[str, any]]]:
        """Rank candidate items for a batch of users."""
        # Simulate model inference time
        await asyncio.sleep(0.05)
        
        # In production, use vectorized batch inference:
        # return self.model.batch_predict(user_features_batch, self.CANDIDATE_ITEMS)
        
        return [self._score_items(features) for features in user_features_batch]


# Component 3: Recommendation Service (Orchestrator)
@serve.deployment
class RecommendationService:
    """Orchestrates the recommendation pipeline."""
    
    def __init__(
        self,
        user_feature_extractor: DeploymentHandle,
        ranking_model: DeploymentHandle
    ):
        self.user_feature_extractor = user_feature_extractor
        self.ranking_model = ranking_model
    
    async def __call__(self, request: Request) -> Dict:
        """Generate recommendations for a user."""
        data = await request.json()
        user_id = data["user_id"]
        top_k = data.get("top_k", 5)
        
        # Step 1: Extract user features
        user_features = await self.user_feature_extractor.extract_features.remote(user_id)
        
        # Step 2: Rank candidate items (batched automatically by @serve.batch)
        ranked_items = await self.ranking_model.rank_items.remote(user_features)
        
        # Step 3: Return top-k recommendations
        return {
            "user_id": user_id,
            "recommendations": ranked_items[:top_k]
        }


# Build the application
app = RecommendationService.bind(
    user_feature_extractor=UserFeatureExtractor.bind(),
    ranking_model=ItemRankingModel.bind()
)
