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
    num_replicas=3,
    ray_actor_options={"num_cpus": 2}
)
class ItemRankingModel:
    """Ranks items for a user based on features.
    
    In production, this runs a trained ML model (XGBoost, neural network, etc.).
    For this example, the code uses a simple scoring function.
    """
    
    def __init__(self):
        # In production: Load model weights
        # self.model = load_model("s3://models/ranking_model.pkl")
        pass
    
    async def rank_items(
        self, 
        user_features: Dict[str, float], 
        candidate_items: List[str]
    ) -> List[Dict[str, any]]:
        """Rank candidate items for the user."""
        # Simulate model inference time
        await asyncio.sleep(0.05)
        
        # In production:
        # scores = self.model.predict(user_features, candidate_items)
        
        # Mock scoring: combine user engagement with item popularity
        ranked_items = []
        for item_id in candidate_items:
            # Simple mock scoring based on user engagement and item hash
            item_popularity = (hash(item_id) % 100) / 100.0
            score = (
                user_features["engagement_score"] * 0.6 + 
                item_popularity * 0.4
            )
            ranked_items.append({
                "item_id": item_id,
                "score": round(score, 3)
            })
        
        # Sort by score descending
        ranked_items.sort(key=lambda x: x["score"], reverse=True)
        return ranked_items


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
        candidate_items = data.get("candidate_items", [])
        top_k = data.get("top_k", 5)
        
        # Step 1: Extract user features
        user_features = await self.user_feature_extractor.extract_features.remote(user_id)
        
        # Step 2: Rank candidate items
        ranked_items = await self.ranking_model.rank_items.remote(
            user_features, 
            candidate_items
        )
        
        # Step 3: Return top-k recommendations
        return {
            "user_id": user_id,
            "recommendations": ranked_items[:top_k],
            "total_candidates": len(candidate_items)
        }


# Build the application
app = RecommendationService.bind(
    user_feature_extractor=UserFeatureExtractor.bind(),
    ranking_model=ItemRankingModel.bind()
)
