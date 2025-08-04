# Distributed RAG pipeline



This tutorial covers end-to-end Retrieval-Augmented Generation (RAG) pipelines using [Ray](https://docs.ray.io/), from data ingestion and LLM deployment to prompt engineering, evaluation and scaling out all workloads in the application.

<div align="center">
  <img src="https://images.ctfassets.net/xjan103pcp94/4PX0l1ruKqfH17YvUiMFPw/c60a7a665125cb8056bebcc146c23b76/image8.png" width=600>
</div>

## Notebooks

1. [01_(Optional)_Regular_Document_Processing_Pipeline.ipynb](https://github.com/ray-project/ray/blob/master/doc/source/ray-overview/examples/e2e-rag/notebooks/01_(Optional)_Regular_Document_Processing_Pipeline.ipynb)  
   Demonstrates a baseline document processing workflow for extracting, cleaning, and indexing text prior to RAG.

2. [02_Scalable_RAG_Data_Ingestion_with_Ray_Data.ipynb](https://github.com/ray-project/ray/blob/master/doc/source/ray-overview/examples/e2e-rag/notebooks/02_Scalable_RAG_Data_Ingestion_with_Ray_Data.ipynb)  
   Shows how to build a high-throughput data ingestion pipeline for RAG using Ray Data.

3. [03_Deploy_LLM_with_Ray_Serve.ipynb](https://github.com/ray-project/ray/blob/master/doc/source/ray-overview/examples/e2e-rag/notebooks/03_Deploy_LLM_with_Ray_Serve.ipynb)  
   Guides you through containerizing and serving a large language model at scale with Ray Serve.

4. [04_Build_Basic_RAG_Chatbot](https://github.com/ray-project/ray/blob/master/doc/source/ray-overview/examples/e2e-rag/notebooks/04_Build_Basic_RAG_Chatbot.ipynb)  
   Combines your indexed documents and served LLM to create a simple, interactive RAG chatbot.

5. [05_Improve_RAG_with_Prompt_Engineering](https://github.com/ray-project/ray/blob/master/doc/source/ray-overview/examples/e2e-rag/notebooks/05_Improve_RAG_with_Prompt_Engineering.ipynb)  
   Explores prompt-engineering techniques to boost relevance and accuracy in RAG responses.

6. [06_(Optional)_Evaluate_RAG_with_Online_Inference](https://github.com/ray-project/ray/blob/master/doc/source/ray-overview/examples/e2e-rag/notebooks/06_(Optional)_Evaluate_RAG_with_Online_Inference.ipynb)  
   Provides methods to assess RAG quality in real time through live queries and metrics tracking.

7. [07_Evaluate_RAG_with_Ray_Data_LLM_Batch_inference](https://github.com/ray-project/ray/blob/master/doc/source/ray-overview/examples/e2e-rag/notebooks/07_Evaluate_RAG_with_Ray_Data_LLM_Batch_inference.ipynb)  
   Implements large-scale batch evaluation of RAG outputs using Ray Data + LLM batch inference.

---

> **Note:** Notebooks marked “(Optional)” cover complementary topics and can be skipped if you prefer to focus on the core RAG flow.


```{toctree}
:hidden:

notebooks/01_(Optional)_Regular_Document_Processing_Pipeline
notebooks/02_Scalable_RAG_Data_Ingestion_with_Ray_Data
notebooks/03_Deploy_LLM_with_Ray_Serve
notebooks/04_Build_Basic_RAG_Chatbot
notebooks/05_Improve_RAG_with_Prompt_Engineering
notebooks/06_(Optional)_Evaluate_RAG_with_Online_Inference
notebooks/07_Evaluate_RAG_with_Ray_Data_LLM_Batch_inference

```
