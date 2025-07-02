import asyncio
import json
import random
from random import randint
from typing import AsyncGenerator, Dict, Optional, Any, List, Union

from ray.llm._internal.serve.configs.openai_api_models import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    CompletionRequest,
    CompletionResponse,
    EmbeddingRequest,
    EmbeddingResponse,
    ErrorResponse,
)
from ray.llm._internal.serve.configs.server_models import (
    DiskMultiplexConfig,
    LLMConfig,
)
from ray.llm._internal.serve.deployments.llm.llm_engine import LLMEngine


class MockVLLMEngine(LLMEngine):
    """Mock vLLM Engine that generates fake text responses."""

    def __init__(self, llm_config: LLMConfig):
        """Create a mock vLLM Engine.

        Args:
            llm_config: The llm configuration for this engine
        """
        self.llm_config = llm_config
        self.started = False
        self._current_lora_model: Optional[DiskMultiplexConfig] = None

    async def start(self):
        """Start the mock engine."""
        self.started = True

    async def resolve_lora(self, lora_model: DiskMultiplexConfig):
        """Resolve/load a LoRA model."""
        self._current_lora_model = lora_model

    async def check_health(self) -> None:
        """Check the health of the mock engine."""
        if not self.started:
            raise RuntimeError("Engine not started")

    async def chat(self, request: ChatCompletionRequest) -> AsyncGenerator[Union[str, ChatCompletionResponse, ErrorResponse], None]:
        """Mock chat completion."""
        if not self.started:
            raise RuntimeError("Engine not started")
        
        # Extract prompt text from messages
        prompt_text = ""
        if request.messages:
            for message in request.messages:
                if hasattr(message, 'content') and message.content:
                    prompt_text += str(message.content) + " "
        
        max_tokens = getattr(request, 'max_tokens', None) or randint(1, 10)
        
        # Generate streaming response
        async for response in self._generate_chat_response(
            request=request,
            prompt_text=prompt_text.strip(),
            max_tokens=max_tokens
        ):
            yield response

    async def completions(self, request: CompletionRequest) -> AsyncGenerator[Union[str, CompletionResponse, ErrorResponse], None]:
        """Mock text completion."""
        if not self.started:
            raise RuntimeError("Engine not started")
        
        prompt_text = str(request.prompt) if request.prompt else ""
        max_tokens = getattr(request, 'max_tokens', None) or randint(5, 20)
        
        # Generate streaming response
        async for response in self._generate_completion_response(
            request=request,
            prompt_text=prompt_text,
            max_tokens=max_tokens
        ):
            yield response

    async def embeddings(self, request: EmbeddingRequest) -> AsyncGenerator[Union[str, EmbeddingResponse, ErrorResponse], None]:
        """Mock embeddings generation."""
        if not self.started:
            raise RuntimeError("Engine not started")
        
        # Generate a mock embedding response
        embedding_data = []
        inputs = request.input if isinstance(request.input, list) else [request.input]
        
        for i, text in enumerate(inputs):
            # Generate random embedding vector
            dimensions = getattr(request, 'dimensions', None) or 1536
            embedding = [random.uniform(-1, 1) for _ in range(dimensions)]
            
            embedding_data.append({
                "object": "embedding",
                "embedding": embedding,
                "index": i
            })
        
        response = EmbeddingResponse(
            object="list",
            data=embedding_data,
            model=getattr(request, 'model', 'mock-model'),
            usage={
                "prompt_tokens": len(str(request.input).split()),
                "total_tokens": len(str(request.input).split())
            }
        )
        yield response

    async def _generate_chat_response(
        self, 
        request: ChatCompletionRequest, 
        prompt_text: str, 
        max_tokens: int
    ) -> AsyncGenerator[Union[str, ChatCompletionResponse], None]:
        """Generate mock chat completion response."""
        
        request_id = request.request_id or f"chatcmpl-{random.randint(1000, 9999)}"
        if request.stream:
            # Streaming response - return SSE formatted strings
            created_time = int(asyncio.get_event_loop().time())
            model_name = getattr(request, 'model', 'mock-model')
            
            for i in range(max_tokens):
                token = f"test_{i} "
                if i == max_tokens - 1:
                    # no space for the last token
                    token = f"test_{i}"
                
                # Create streaming chunk
                choice = {
                    "index": 0,
                    "delta": {
                        "content": token,
                        "role": "assistant" if i == 0 else None
                    },
                    "finish_reason": "stop" if i == max_tokens - 1 else None
                }
                
                chunk_data = {
                    "id": request_id,
                    "object": "chat.completion.chunk",
                    "created": created_time,
                    "model": model_name,
                    "choices": [choice]
                }
                
                # Format as SSE
                yield f"data: {json.dumps(chunk_data)}\n\n"
                await asyncio.sleep(0.01)  # Simulate processing time
            
            # Send final [DONE] message
            yield "data: [DONE]\n\n"
        else:
            # Non-streaming response - return response object
            generated_text = " ".join([f"test_{i}" for i in range(max_tokens)])
            
            choice = {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": generated_text
                },
                "finish_reason": "stop"
            }
            
            response = ChatCompletionResponse(
                id=request_id,
                object="chat.completion",
                created=int(asyncio.get_event_loop().time()),
                model=getattr(request, 'model', 'mock-model'),
                choices=[choice],
                usage={
                    "prompt_tokens": len(prompt_text.split()),
                    "completion_tokens": max_tokens,
                    "total_tokens": len(prompt_text.split()) + max_tokens
                }
            )
            
            yield response

    async def _generate_completion_response(
        self, 
        request: CompletionRequest, 
        prompt_text: str, 
        max_tokens: int
    ) -> AsyncGenerator[Union[str, CompletionResponse], None]:
        """Generate mock completion response."""
        
        request_id = request.request_id or f"cmpl-{random.randint(1000, 9999)}"
        if request.stream:
            # Streaming response - return SSE formatted strings
            created_time = int(asyncio.get_event_loop().time())
            model_name = getattr(request, 'model', 'mock-model')
            
            for i in range(max_tokens):
                token = f"test_{i} "
                if i == max_tokens - 1:
                    # no space for the last token
                    token = f"test_{i}"
                
                choice = {
                    "index": 0,
                    "text": token,
                    "finish_reason": "stop" if i == max_tokens - 1 else None
                }
                
                chunk_data = {
                    "id": request_id,
                    "object": "text_completion",
                    "created": created_time,
                    "model": model_name,
                    "choices": [choice]
                }
                
                # Format as SSE
                yield f"data: {json.dumps(chunk_data)}\n\n"
                await asyncio.sleep(0.01)
            
            # Send final [DONE] message
            yield "data: [DONE]\n\n"
        else:
            # Non-streaming response - return response object
            generated_text = " ".join([f"test_{i}" for i in range(max_tokens)])
            
            choice = {
                "index": 0,
                "text": generated_text,
                "finish_reason": "stop"
            }
            
            response = CompletionResponse(
                id=request_id,
                object="text_completion",
                created=int(asyncio.get_event_loop().time()),
                model=getattr(request, 'model', 'mock-model'),
                choices=[choice],
                usage={
                    "prompt_tokens": len(prompt_text.split()),
                    "completion_tokens": max_tokens,
                    "total_tokens": len(prompt_text.split()) + max_tokens
                }
            )
            
            yield response


# class MockEchoVLLMEngine(MockVLLMEngine):
#     """Mock engine that responds with information about the request sent to it.
    
#     Useful for testing the contents of requests created in data plane code.
#     """

#     async def chat(self, request: ChatCompletionRequest) -> AsyncGenerator[Union[str, ChatCompletionResponse, ErrorResponse], None]:
#         """Echo the chat request information."""
#         if not self.started:
#             raise RuntimeError("Engine not started")
        
#         # Convert request to JSON for echoing
#         request_info = {
#             "request_type": "chat",
#             "model": getattr(request, 'model', None),
#             "messages": getattr(request, 'messages', []),
#             "max_tokens": getattr(request, 'max_tokens', None),
#             "temperature": getattr(request, 'temperature', None),
#             "stream": getattr(request, 'stream', False),
#             "current_lora_model": self._current_lora_model.model_dump() if self._current_lora_model else None
#         }
        
#         echo_text = json.dumps(request_info, indent=2)
        
#         if request.stream:
#             # Return as SSE for streaming
#             chunk_data = {
#                 "id": f"chatcmpl-echo-{random.randint(1000, 9999)}",
#                 "object": "chat.completion.chunk",
#                 "created": int(asyncio.get_event_loop().time()),
#                 "model": getattr(request, 'model', 'mock-echo-model'),
#                 "choices": [{
#                     "index": 0,
#                     "delta": {
#                         "role": "assistant",
#                         "content": echo_text
#                     },
#                     "finish_reason": "stop"
#                 }]
#             }
#             yield f"data: {json.dumps(chunk_data)}\n\n"
#             yield "data: [DONE]\n\n"
#         else:
#             # Return as response object
#             choice = {
#                 "index": 0,
#                 "message": {
#                     "role": "assistant",
#                     "content": echo_text
#                 },
#                 "finish_reason": "stop"
#             }
            
#             response = ChatCompletionResponse(
#                 id=f"chatcmpl-echo-{random.randint(1000, 9999)}",
#                 object="chat.completion",
#                 created=int(asyncio.get_event_loop().time()),
#                 model=getattr(request, 'model', 'mock-echo-model'),
#                 choices=[choice]
#             )
            
#             yield response

#     async def completions(self, request: CompletionRequest) -> AsyncGenerator[Union[str, CompletionResponse, ErrorResponse], None]:
#         """Echo the completion request information."""
#         if not self.started:
#             raise RuntimeError("Engine not started")
        
#         request_info = {
#             "request_type": "completion",
#             "model": getattr(request, 'model', None),
#             "prompt": getattr(request, 'prompt', None),
#             "max_tokens": getattr(request, 'max_tokens', None),
#             "temperature": getattr(request, 'temperature', None),
#             "stream": getattr(request, 'stream', False),
#             "current_lora_model": self._current_lora_model.model_dump() if self._current_lora_model else None
#         }
        
#         echo_text = json.dumps(request_info, indent=2)
        
#         if request.stream:
#             # Return as SSE for streaming
#             chunk_data = {
#                 "id": f"cmpl-echo-{random.randint(1000, 9999)}",
#                 "object": "text_completion",
#                 "created": int(asyncio.get_event_loop().time()),
#                 "model": getattr(request, 'model', 'mock-echo-model'),
#                 "choices": [{
#                     "index": 0,
#                     "text": echo_text,
#                     "finish_reason": "stop"
#                 }]
#             }
#             yield f"data: {json.dumps(chunk_data)}\n\n"
#             yield "data: [DONE]\n\n"
#         else:
#             # Return as response object
#             choice = {
#                 "index": 0,
#                 "text": echo_text,
#                 "finish_reason": "stop"
#             }
            
#             response = CompletionResponse(
#                 id=f"cmpl-echo-{random.randint(1000, 9999)}",
#                 object="text_completion",
#                 created=int(asyncio.get_event_loop().time()),
#                 model=getattr(request, 'model', 'mock-echo-model'),
#                 choices=[choice]
#             )
            
#             yield response

#     async def embeddings(self, request: EmbeddingRequest) -> AsyncGenerator[Union[str, EmbeddingResponse, ErrorResponse], None]:
#         """Echo the embedding request information."""
#         if not self.started:
#             raise RuntimeError("Engine not started")
        
#         request_info = {
#             "request_type": "embedding",
#             "model": getattr(request, 'model', None),
#             "input": getattr(request, 'input', None),
#             "encoding_format": getattr(request, 'encoding_format', None),
#             "dimensions": getattr(request, 'dimensions', None),
#             "current_lora_model": self._current_lora_model.model_dump() if self._current_lora_model else None
#         }
        
#         # Return request info as mock embedding
#         echo_text = json.dumps(request_info, indent=2)
#         mock_embedding = [float(ord(c)) for c in echo_text[:10]]  # Mock embedding from first 10 chars
        
#         response = EmbeddingResponse(
#             object="list",
#             data=[{
#                 "object": "embedding",
#                 "embedding": mock_embedding,
#                 "index": 0
#             }],
#             model=getattr(request, 'model', 'mock-echo-model'),
#             usage={
#                 "prompt_tokens": len(str(request.input).split()),
#                 "total_tokens": len(str(request.input).split())
#             }
#         )
        
#         yield response


# class MockMultiplexEngine(MockVLLMEngine):
#     """Mock engine for testing multiplex/LoRA functionality."""

#     def __init__(self, llm_config: LLMConfig):
#         super().__init__(llm_config)
#         self.loaded_lora_models: List[DiskMultiplexConfig] = []

#     async def resolve_lora(self, lora_model: DiskMultiplexConfig):
#         """Mock LoRA model loading."""
#         self._current_lora_model = lora_model
#         # Keep track of loaded models
#         if lora_model not in self.loaded_lora_models:
#             self.loaded_lora_models.append(lora_model)

#     async def chat(self, request: ChatCompletionRequest) -> AsyncGenerator[Union[str, ChatCompletionResponse, ErrorResponse], None]:
#         """Chat with multiplex information."""
#         if not self.started:
#             raise RuntimeError("Engine not started")
        
#         # Include multiplex info in response
#         lora_info = ""
#         if self._current_lora_model:
#             lora_info = f" [LoRA: {self._current_lora_model.model_id}]"
        
#         generated_text = f"Mock multiplex response{lora_info}"
        
#         if request.stream:
#             # Return as SSE for streaming
#             chunk_data = {
#                 "id": f"chatcmpl-multiplex-{random.randint(1000, 9999)}",
#                 "object": "chat.completion.chunk",
#                 "created": int(asyncio.get_event_loop().time()),
#                 "model": getattr(request, 'model', 'mock-multiplex-model'),
#                 "choices": [{
#                     "index": 0,
#                     "delta": {
#                         "role": "assistant",
#                         "content": generated_text
#                     },
#                     "finish_reason": "stop"
#                 }]
#             }
#             yield f"data: {json.dumps(chunk_data)}\n\n"
#             yield "data: [DONE]\n\n"
#         else:
#             # Return as response object
#             choice = {
#                 "index": 0,
#                 "message": {
#                     "role": "assistant",
#                     "content": generated_text
#                 },
#                 "finish_reason": "stop"
#             }
            
#             response = ChatCompletionResponse(
#                 id=f"chatcmpl-multiplex-{random.randint(1000, 9999)}",
#                 object="chat.completion",
#                 created=int(asyncio.get_event_loop().time()),
#                 model=getattr(request, 'model', 'mock-multiplex-model'),
#                 choices=[choice]
#             )
            
#             yield response


# class MockJSONModeVLLMEngine(MockVLLMEngine):
#     """Mock engine that generates valid JSON responses when JSON mode is requested."""

#     async def chat(self, request: ChatCompletionRequest) -> AsyncGenerator[Union[str, ChatCompletionResponse, ErrorResponse], None]:
#         """Generate JSON or text response based on request format."""
#         if not self.started:
#             raise RuntimeError("Engine not started")
        
#         # Check if JSON mode is requested
#         response_format = getattr(request, 'response_format', None)
#         is_json_mode = (
#             response_format and 
#             hasattr(response_format, 'type') and 
#             response_format.type == "json_object"
#         )
        
#         if is_json_mode:
#             # Generate valid JSON based on schema if provided
#             if hasattr(response_format, 'json_schema') and response_format.json_schema:
#                 try:
#                     # Use the schema to generate a valid response
#                     json_response = generate_from_schema(response_format.json_schema)
#                     generated_text = json.dumps(json_response, ensure_ascii=False)
#                 except Exception as e:
#                     # Fallback to default JSON if schema generation fails
#                     json_response = {
#                         "error": f"Schema generation failed: {str(e)}",
#                         "schema_provided": bool(response_format.json_schema),
#                         "fallback_response": True
#                     }
#                     generated_text = json.dumps(json_response, indent=2)
#             else:
#                 # Default JSON response when no schema is provided
#                 json_response = {
#                     "message": "This is a mock JSON response",
#                     "timestamp": int(asyncio.get_event_loop().time()),
#                     "request_info": {
#                         "model": getattr(request, 'model', 'unknown'),
#                         "has_messages": bool(getattr(request, 'messages', [])),
#                         "lora_model": self._current_lora_model.model_id if self._current_lora_model else None
#                     }
#                 }
#                 generated_text = json.dumps(json_response, indent=2)
#         else:
#             # Generate regular text
#             generated_text = "Mock response from JSON mode engine"
        
#         if request.stream:
#             # Return as SSE for streaming with realistic JSON chunking
#             request_id = f"chatcmpl-json-{random.randint(1000, 9999)}"
#             created_time = int(asyncio.get_event_loop().time())
#             model_name = getattr(request, 'model', 'mock-json-model')
            
#             if is_json_mode:
#                 # For JSON streaming, split the JSON into realistic chunks
#                 # This simulates how a real LLM would generate JSON token by token
#                 max_chunk_size = 10  # Characters per chunk
#                 chunks = [generated_text[i:i+max_chunk_size] for i in range(0, len(generated_text), max_chunk_size)]
                
#                 for i, chunk in enumerate(chunks):
#                     chunk_data = {
#                         "id": request_id,
#                         "object": "chat.completion.chunk",
#                         "created": created_time,
#                         "model": model_name,
#                         "choices": [{
#                             "index": 0,
#                             "delta": {
#                                 "content": chunk,
#                                 "role": "assistant" if i == 0 else None
#                             },
#                             "finish_reason": "stop" if i == len(chunks) - 1 else None
#                         }]
#                     }
#                     yield f"data: {json.dumps(chunk_data)}\n\n"
#                     await asyncio.sleep(0.01)  # Simulate processing time
#             else:
#                 # For non-JSON streaming, return as single chunk
#                 chunk_data = {
#                     "id": request_id,
#                     "object": "chat.completion.chunk",
#                     "created": created_time,
#                     "model": model_name,
#                     "choices": [{
#                         "index": 0,
#                         "delta": {
#                             "role": "assistant",
#                             "content": generated_text
#                         },
#                         "finish_reason": "stop"
#                     }]
#                 }
#                 yield f"data: {json.dumps(chunk_data)}\n\n"
            
#             # Send final [DONE] message
#             yield "data: [DONE]\n\n"
#         else:
#             # Return as response object
#             choice = {
#                 "index": 0,
#                 "message": {
#                     "role": "assistant",
#                     "content": generated_text
#                 },
#                 "finish_reason": "stop"
#             }
            
#             response = ChatCompletionResponse(
#                 id=f"chatcmpl-json-{random.randint(1000, 9999)}",
#                 object="chat.completion",
#                 created=int(asyncio.get_event_loop().time()),
#                 model=getattr(request, 'model', 'mock-json-model'),
#                 choices=[choice]
#             )
            
#             yield response


# class MockPDDisaggVLLMEngine(MockVLLMEngine):
#     """Mock engine for testing Prefill/Decode disaggregated functionality."""

#     def __init__(self, llm_config: LLMConfig):
#         super().__init__(llm_config)
#         self.prefill_cache = {}
#         self.kv_transfer_enabled = False

#     async def start(self):
#         """Start with disaggregation support."""
#         await super().start()
#         # Mock enabling KV transfer
#         self.kv_transfer_enabled = True

#     async def chat(self, request: ChatCompletionRequest) -> AsyncGenerator[Union[str, ChatCompletionResponse, ErrorResponse], None]:
#         """Chat with disaggregation simulation."""
#         if not self.started:
#             raise RuntimeError("Engine not started")
        
#         # Simulate prefill/decode disaggregation
#         request_id = getattr(request, 'request_id', f"req-{random.randint(1000, 9999)}")
        
#         # Mock prefill phase
#         prompt_text = ""
#         if hasattr(request, 'messages') and request.messages:
#             for message in request.messages:
#                 if hasattr(message, 'content') and message.content:
#                     prompt_text += str(message.content) + " "
        
#         # Cache prefill result
#         self.prefill_cache[request_id] = {
#             "prompt": prompt_text.strip(),
#             "kv_cache": f"mock_kv_cache_{len(prompt_text)}"
#         }
        
#         # Mock decode phase
#         generated_text = f"Mock PD disagg response [cached: {request_id}]"
#         if self.kv_transfer_enabled:
#             generated_text += " [KV transfer enabled]"
        
#         if request.stream:
#             # Return as SSE for streaming
#             chunk_data = {
#                 "id": f"chatcmpl-pd-{request_id}",
#                 "object": "chat.completion.chunk",
#                 "created": int(asyncio.get_event_loop().time()),
#                 "model": getattr(request, 'model', 'mock-pd-model'),
#                 "choices": [{
#                     "index": 0,
#                     "delta": {
#                         "role": "assistant",
#                         "content": generated_text
#                     },
#                     "finish_reason": "stop"
#                 }]
#             }
#             yield f"data: {json.dumps(chunk_data)}\n\n"
#             yield "data: [DONE]\n\n"
#         else:
#             # Return as response object
#             choice = {
#                 "index": 0,
#                 "message": {
#                     "role": "assistant", 
#                     "content": generated_text
#                 },
#                 "finish_reason": "stop"
#             }
            
#             response = ChatCompletionResponse(
#                 id=f"chatcmpl-pd-{request_id}",
#                 object="chat.completion",
#                 created=int(asyncio.get_event_loop().time()),
#                 model=getattr(request, 'model', 'mock-pd-model'),
#                 choices=[choice]
#             )
            
#             yield response


# class FakeLoraModelLoader:
#     """Fake LoRA model loader for testing."""

#     async def load_model(self, lora_model_id: str, llm_config: LLMConfig) -> DiskMultiplexConfig:
#         """Load a fake LoRA model."""
#         return DiskMultiplexConfig(
#             model_id=lora_model_id,
#             max_total_tokens=llm_config.max_request_context_length,
#             local_path="/fake/local/path",
#             lora_assigned_int_id=random.randint(1, 100),
#         )


# # Utility functions for JSON generation and validation
# def generate_from_schema(schema: dict) -> Any:
#     """Generate mock data from JSON schema."""
#     if "type" not in schema:
#         raise ValueError("Schema must have a 'type' property")

#     # Handle enum values first (takes precedence over type)
#     if "enum" in schema:
#         return random.choice(schema["enum"])

#     # Handle const values
#     if "const" in schema:
#         return schema["const"]

#     schema_type = schema["type"]

#     if schema_type == "object":
#         obj = {}
#         properties = schema.get("properties", {})
#         required = schema.get("required", [])
        
#         # Generate required properties first
#         for prop in required:
#             if prop in properties:
#                 obj[prop] = generate_from_schema(properties[prop])
        
#         # Generate optional properties (randomly include some)
#         for prop, prop_schema in properties.items():
#             if prop not in obj and random.choice([True, False]):
#                 obj[prop] = generate_from_schema(prop_schema)
        
#         return obj

#     elif schema_type == "array":
#         item_schema = schema.get("items", {"type": "string"})
#         min_items = schema.get("minItems", 1)
#         max_items = schema.get("maxItems", 5)
#         array_length = random.randint(min_items, max_items)
        
#         return [generate_from_schema(item_schema) for _ in range(array_length)]

#     elif schema_type == "string":
#         # Handle string patterns and formats
#         if "pattern" in schema:
#             # For testing purposes, return a string that might match common patterns
#             pattern = schema["pattern"]
#             if "email" in pattern.lower() or "@" in pattern:
#                 return "test@example.com"
#             elif "phone" in pattern.lower() or "\\d" in pattern:
#                 return "123-456-7890"
#             else:
#                 return "pattern_match_string"
        
#         if "format" in schema:
#             format_type = schema["format"]
#             if format_type == "email":
#                 return "test@example.com"
#             elif format_type == "date":
#                 return "2024-01-15"
#             elif format_type == "date-time":
#                 return "2024-01-15T10:30:00Z"
#             elif format_type == "uri":
#                 return "https://example.com"
#             elif format_type == "uuid":
#                 return "550e8400-e29b-41d4-a716-446655440000"
        
#         # Handle string length constraints
#         min_length = schema.get("minLength", 1)
#         max_length = schema.get("maxLength", 20)
#         base_string = "mock_string_value"
        
#         if max_length < len(base_string):
#             return base_string[:max_length]
#         elif min_length > len(base_string):
#             return base_string + "x" * (min_length - len(base_string))
#         else:
#             return base_string

#     elif schema_type == "integer":
#         minimum = schema.get("minimum", 0)
#         maximum = schema.get("maximum", 100)
#         return random.randint(minimum, maximum)

#     elif schema_type == "number":
#         minimum = schema.get("minimum", 0.0)
#         maximum = schema.get("maximum", 100.0)
#         return random.uniform(minimum, maximum)

#     elif schema_type == "boolean":
#         return random.choice([True, False])

#     elif schema_type == "null":
#         return None

#     # Handle multiple types (anyOf, oneOf)
#     elif isinstance(schema_type, list):
#         chosen_type = random.choice(schema_type)
#         return generate_from_schema({"type": chosen_type})

#     else:
#         raise ValueError(f"Unsupported schema type: {schema_type}")


# def validate_json_schema_response(response_text: str, schema: dict) -> bool:
#     """
#     Validate that a JSON response conforms to the provided schema.
#     This is a simple validation for testing purposes.
#     """
#     try:
#         data = json.loads(response_text)
#         # Basic validation - in a real implementation you'd use jsonschema library
#         return _validate_against_schema(data, schema)
#     except (json.JSONDecodeError, Exception):
#         return False


# def _validate_against_schema(data: Any, schema: dict) -> bool:
#     """Helper function for basic schema validation."""
#     schema_type = schema.get("type")
    
#     if schema_type == "object" and isinstance(data, dict):
#         # Check required properties
#         required = schema.get("required", [])
#         for prop in required:
#             if prop not in data:
#                 return False
        
#         # Check property types
#         properties = schema.get("properties", {})
#         for prop, value in data.items():
#             if prop in properties:
#                 if not _validate_against_schema(value, properties[prop]):
#                     return False
#         return True
    
#     elif schema_type == "array" and isinstance(data, list):
#         item_schema = schema.get("items", {})
#         return all(_validate_against_schema(item, item_schema) for item in data)
    
#     elif schema_type == "string" and isinstance(data, str):
#         return True
    
#     elif schema_type == "integer" and isinstance(data, int):
#         return True
    
#     elif schema_type == "number" and isinstance(data, (int, float)):
#         return True
    
#     elif schema_type == "boolean" and isinstance(data, bool):
#         return True
    
#     elif schema_type == "null" and data is None:
#         return True
    
#     return False


# def split_string_into_chunks(s: str, n: int) -> List[str]:
#     """Split string into n chunks."""
#     if n <= 0:
#         raise ValueError("Number of chunks must be greater than 0")

#     chunk_size = len(s) // n
#     remainder = len(s) % n

#     chunks = []
#     start = 0
#     for i in range(n):
#         end = start + chunk_size + (1 if i < remainder else 0)
#         chunks.append(s[start:end])
#         start = end

#     return chunks


# def get_prompt_length(prompt: Union[str, List[str], List[int]]) -> int:
#     """Get the length of a prompt."""
#     if isinstance(prompt, str):
#         return len(prompt.split())
#     elif isinstance(prompt, list):
#         return len(prompt)
#     else:
#         return 0
