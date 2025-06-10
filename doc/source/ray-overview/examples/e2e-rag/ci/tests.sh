#!/bin/bash

# don't use nbcovert or jupytext unless you're willing
# to check each subprocess unit and validate that errors
# aren't being consumed/hidden

ordered_notebook_names=(
  #"01_(Optional)_Regular_Document_Processing_Pipeline"
  #"02_Scalable_RAG_Data_Ingestion_with_Ray_Data"
  "03_Deploy_LLM_with_Ray_Serve"
  # "04_Build_Basic_RAG_Chatbot"                         # skipped - requires bespoke Anyscale service
  # "05_Improve_RAG_with_Prompt_Engineering"             # skipped - requires bespoke Anyscale service
  # "06_(Optional)_Evaluate_RAG_with_Online_Inference"   # skipped - requires bespoke Anyscale service
  "07_Evaluate_RAG_with_Ray_Data_LLM_Batch_inference"
)

for nb in "${ordered_notebook_names[@]}"; do
  python ci/nb2py.py notebooks/${nb}.ipynb notebooks/${nb}.py  # convert notebook to script
  (cd notebooks && python ${nb}.py)  # run generated script
  (cd notebooks && rm ${nb}.py)  # remove the generated script
done
