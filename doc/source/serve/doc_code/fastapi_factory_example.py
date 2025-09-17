"""
FastAPI Factory Pattern Example for Ray Serve

This example demonstrates the FastAPI factory pattern which allows
using third-party FastAPI plugins without serialization issues.
"""

import time
from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from ray import serve


class TextRequest(BaseModel):
    text: str


class PredictionResponse(BaseModel):
    prediction: str
    confidence: float
    processing_time: float


@serve.deployment
class TextClassifier:
    """Mock text classifier deployment."""
    
    def __init__(self):
        # Initialize your model here
        self.model = "mock-classifier"
    
    def predict(self, text: str):
        """Mock prediction logic."""
        return {
            "prediction": "positive" if len(text) > 10 else "negative",
            "confidence": 0.95
        }


def fastapi_factory():
    """
    Factory function that creates and configures a FastAPI app.
    
    This function is evaluated on each replica, avoiding cloudpickle
    serialization issues with third-party plugins.
    """
    app = FastAPI(
        title="Text Classification API",
        description="API for text classification using Ray Serve",
        version="1.0.0"
    )
    
    # Middleware for timing
    @app.middleware("http")
    async def add_process_time_header(request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        return response
    
    # Exception handler
    @app.exception_handler(ValueError)
    async def value_error_handler(request, exc):
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid input", "detail": str(exc)}
        )
    
    # Dependency for getting classifier handle
    def get_classifier():
        return serve.get_deployment_handle("TextClassifier", "default")
    
    @app.get("/health")
    def health_check():
        """Health check endpoint."""
        return {"status": "healthy", "service": "text-classifier"}
    
    @app.post("/predict", response_model=PredictionResponse)
    async def predict(
        request: TextRequest,
        classifier = Depends(get_classifier)
    ):
        """Predict sentiment of input text."""
        if not request.text.strip():
            raise ValueError("Text cannot be empty")
        
        start_time = time.time()
        result = await classifier.predict.remote(request.text)
        processing_time = time.time() - start_time
        
        return PredictionResponse(
            prediction=result["prediction"],
            confidence=result["confidence"],
            processing_time=processing_time
        )
    
    return app


@serve.deployment
@serve.ingress(fastapi_factory)
class TextClassificationAPI:
    """Main API deployment using FastAPI factory pattern."""
    
    def __init__(self, classifier_handle):
        self.classifier_handle = classifier_handle


# Example usage
if __name__ == "__main__":
    # Deploy the application
    classifier = TextClassifier.options(name="TextClassifier")
    api = TextClassificationAPI.bind(classifier.bind())
    serve.run(api)
    
    # Test the API
    import requests
    
    # Health check
    response = requests.get("http://localhost:8000/health")
    print("Health check:", response.json())
    
    # Prediction
    response = requests.post(
        "http://localhost:8000/predict",
        json={"text": "This is a great example of the factory pattern!"}
    )
    print("Prediction:", response.json())
    
    print("\nThis demonstrates the FastAPI factory pattern in Ray Serve!")