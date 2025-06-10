#!/bin/bash

# Notebooks 4, 5 and 6 require bespoke Anyscale Service (so skip testing)

ordered_notebook_names=(
  "01_(Optional)_Regular_Document_Processing_Pipeline"
  "02_Scalable_RAG_Data_Ingestion_with_Ray_Data"
  "03_Deploy_LLM_with_Ray_Serve"
  "07_Evaluate_RAG_with_Ray_Data_LLM_Batch_inference"
)

for nb in "${ordered_notebook_names[@]}"; do
  python ci/nb2py.py "notebooks/${nb}.ipynb" "notebooks/${nb}.py"
  (cd notebooks && python "${nb}.py")
  (cd notebooks && rm "${nb}.py")
done
