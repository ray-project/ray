(serve-fastapi-factory-pattern)=
# FastAPI Factory Pattern

The FastAPI factory pattern in Ray Serve provides additional flexibility for integrating FastAPI applications, especially when using third-party plugins or migrating existing FastAPI apps. This pattern allows you to define FastAPI applications through factory functions that are evaluated on each replica, avoiding cloudpickle serialization issues and enabling seamless integration of existing FastAPI codebases.

## When to Use the Factory Pattern

The factory pattern is particularly beneficial in these scenarios:

### **Migrating Existing FastAPI Applications**
If you have an existing FastAPI application that you want to deploy with Ray Serve without rewriting everything into deployment classes, the factory pattern allows you to integrate your existing codebase with minimal changes.

### **Third-Party Plugin Integration**
Ray Serve's object-based pattern requires FastAPI objects to be serializable via cloudpickle, which prevents the use of standard libraries like `FastAPIInstrumentor` due to their reliance on non-serializable components such as thread locks. The factory pattern evaluates the FastAPI creation on each replica, avoiding these serialization issues.

### **Complex FastAPI Configurations**
When you have sophisticated FastAPI setups with multiple middlewares, custom exception handlers, or complex dependency injection that would be cumbersome to recreate within deployment classes.

**Note**: The factory pattern is an additional option for flexibility. The standard object-based pattern remains the recommended approach for most use cases and doesn't require migration.

## Comparison: Object-based vs. Factory Pattern

### Object-based Pattern (Recommended)

```python
from fastapi import FastAPI
from ray import serve

app = FastAPI()

@serve.deployment
@serve.ingress(app)  # Pass FastAPI object directly
class ASGIIngress:
    @app.get("/class_route")
    def class_route(self):
        return "hello class route"

serve.run(ASGIIngress.bind())
```

### Factory Ingress API (Factory Function)

```python
from fastapi import FastAPI
from ray import serve

def fastapi_factory():
    app = FastAPI()
    
    # Configure routes, middleware, and plugins without serialization concerns
    @app.get("/route")
    def route_handler():
        return "hello route"
    
    # Add middleware, exception handlers, etc.
    @app.middleware("http")
    async def add_middleware(request, call_next):
        response = await call_next(request)
        response.headers["X-Custom"] = "header"
        return response
    
    return app

# Using default ingress deployment class
deployment = serve.deployment(serve.ingress(fastapi_factory)())
serve.run(deployment.bind())
```

## Usage Patterns

### 1. Migrating Existing FastAPI Applications

The factory pattern makes it easy to integrate existing FastAPI applications with Ray Serve without major rewrites:

```python
# Your existing FastAPI app
def create_existing_app():
    app = FastAPI()
    
    # Your existing routes, middleware, etc.
    @app.get("/users/{user_id}")
    def get_user(user_id: int):
        return {"user_id": user_id, "name": f"User {user_id}"}
    
    @app.post("/users")
    def create_user(user_data: dict):
        return {"message": "User created", "data": user_data}
    
    # Existing middleware
    @app.middleware("http")
    async def log_requests(request, call_next):
        print(f"Request: {request.method} {request.url}")
        return await call_next(request)
    
    return app

# Deploy with Ray Serve using factory pattern
@serve.deployment
@serve.ingress(create_existing_app)
class ExistingAppDeployment:
    pass

serve.run(ExistingAppDeployment.bind())
```

### 2. Simple Factory Pattern with Default Deployment

The simplest approach uses the default ingress deployment class:

```python
def fastapi_factory():
    app = FastAPI(docs_url="/custom-docs")
    
    @app.get("/")
    def root():
        return {"message": "Hello World"}
    
    @app.get("/health")
    def health_check():
        return {"status": "healthy"}
    
    return app

# Create deployment with default ingress class
ingress_deployment = serve.deployment(serve.ingress(fastapi_factory)())
serve.run(ingress_deployment.bind())
```

### 3. Deployment Composition with Factory Pattern

The factory pattern works seamlessly with deployment composition:

```python
@serve.deployment
class ModelDeployment:
    def __call__(self, data: str):
        return f"Processed: {data}"

def fastapi_factory():
    app = FastAPI()
    
    def get_model_handle():
        return serve.get_deployment_handle("ModelDeployment", "default")
    
    @app.post("/predict")
    async def predict(
        data: str,
        model: serve.DeploymentHandle = Depends(get_model_handle)
    ):
        result = await model.remote(data)
        return {"prediction": result}
    
    return app

@serve.deployment
@serve.ingress(fastapi_factory)
class APIGateway:
    def __init__(self, model_deployment):
        self.model_deployment = model_deployment

model = ModelDeployment.options(name="ModelDeployment")
gateway = APIGateway.bind(model.bind())
serve.run(gateway)
```

**Note**: In the factory pattern, FastAPI routes access other deployments through `serve.get_deployment_handle()` since they don't have access to the deployment class's `self` context. This is different from the object-based pattern where routes can directly access `self.model_deployment` or other class attributes.

## Advanced Features

### Middleware and Exception Handlers

The factory pattern supports all FastAPI features including middleware and custom exception handlers:

```python
from fastapi import HTTPException
from fastapi.responses import JSONResponse

def fastapi_factory():
    app = FastAPI()
    
    # Custom exception handler
    @app.exception_handler(ValueError)
    async def value_error_handler(request, exc):
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid input", "detail": str(exc)}
        )
    
    # Middleware
    @app.middleware("http")
    async def logging_middleware(request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        return response
    
    @app.get("/")
    def root():
        return {"message": "Hello World"}
    
    @app.get("/error")
    def trigger_error():
        raise ValueError("This is a test error")
    
    return app
```

### Third-Party Plugin Integration

The factory pattern enables seamless integration with third-party plugins:

```python
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

def fastapi_factory():
    app = FastAPI()
    
    # This would fail with the object-based approach due to serialization
    FastAPIInstrumentor.instrument_app(app)
    
    @app.get("/")
    def root():
        return {"message": "Instrumented endpoint"}
    
    return app
```

### Dependency Injection with Deployment Handles

Use FastAPI's dependency injection system to access other deployments:

```python
from fastapi import Depends
from pydantic import BaseModel

class PredictionRequest(BaseModel):
    text: str

def fastapi_factory():
    app = FastAPI()
    
    def get_text_classifier():
        return serve.get_deployment_handle("TextClassifier", "default")
    
    def get_sentiment_analyzer():
        return serve.get_deployment_handle("SentimentAnalyzer", "default")
    
    @app.post("/classify")
    async def classify_text(
        request: PredictionRequest,
        classifier = Depends(get_text_classifier),
        sentiment = Depends(get_sentiment_analyzer)
    ):
        classification = await classifier.predict.remote(request.text)
        sentiment_score = await sentiment.analyze.remote(request.text)
        
        return {
            "text": request.text,
            "classification": classification,
            "sentiment": sentiment_score
        }
    
    return app
```

## Limitations and Considerations

### Route Definition Restrictions

When using the factory pattern, route definitions are limited to the FastAPI application within the factory function. Class-based routes on the deployment class are not supported:

```python
def fastapi_factory():
    app = FastAPI()
    
    @app.get("/allowed")  # ✅ This works
    def allowed_route():
        return "OK"
    
    return app

@serve.deployment
@serve.ingress(fastapi_factory)
class ASGIIngress:
    # ❌ Class-based routes are not supported with factory pattern
    # @app.get("/not-allowed")  
    # def not_allowed(self):
    #     return "Not OK"
    pass
```

### Model Weight Access

With the factory function approach, accessing model weights within routes requires using deployment handles, as the `self` context of the deployment is not available within the FastAPI routes:

```python
def fastapi_factory():
    app = FastAPI()
    
    @app.get("/predict")
    async def predict():
        # ❌ Cannot access self.model directly
        # Must use deployment handles instead
        handle = serve.get_deployment_handle("ModelDeployment", "default")
        return await handle.predict.remote()
    
    return app
```

## Best Practices

### **Choosing the Right Pattern**
1. **Start with object-based:** Use the standard object-based pattern for new applications unless you specifically need factory pattern benefits
2. **Use factory for migration:** Choose factory pattern when integrating existing FastAPI applications
3. **Consider plugin needs:** Use factory pattern if you need third-party FastAPI plugins

## Complete Example

Here's a comprehensive example demonstrating the FastAPI factory pattern:

```{literalinclude} ../doc_code/fastapi_factory_example.py
:language: python
```

This example demonstrates:
- Factory function with comprehensive FastAPI configuration
- Middleware and exception handling
- Dependency injection for deployment handles
- Proper type hints and response models
- Integration with other deployments
- Production-ready error handling

The FastAPI factory pattern provides a robust solution for complex FastAPI integrations while maintaining the full power and flexibility of both Ray Serve and FastAPI.
