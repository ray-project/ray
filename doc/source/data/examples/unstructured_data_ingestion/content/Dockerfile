# Example image Dockerfile for the Unstructured Data Ingestion example. 
# Use this image to spin-up all your workers.

# Start with an Anyscale base image.
FROM anyscale/ray:2.51.1-slim-py312

# Install system dependencies
RUN sudo apt-get update -y \
    && sudo apt-get install --no-install-recommends -y libgl1-mesa-glx libmagic1 poppler-utils tesseract-ocr libreoffice \
    && sudo rm -f /etc/apt/sources.list.d/*


RUN pip install --force-reinstall --no-cache-dir "unstructured[all-docs]==0.18.21" "pandas==2.3.3"
