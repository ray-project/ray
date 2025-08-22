# Start with an Anyscale base image.
# Use the drop-down above to browse through all available images.
FROM anyscale/ray:2.48.0-slim-py312-cu128

# Add your pip dependencies here. Disable cache for a smaller image to optimize build and cluster startup time.
# RUN pip install --no-cache-dir --upgrade <package1> <package2>

# Add your Debian packages here. Do not install unnecessary packages for a smaller image size to optimize build and cluster startup time.
# RUN sudo apt-get update -y \
#     && sudo apt-get install --no-install-recommends -y <package1> <package2> \
#     && sudo rm -f /etc/apt/sources.list.d/*

# Add other build commands here.
# RUN echo "Testing Ray import..." && python -c "import ray"
RUN python3 -m pip install --no-cache-dir \
    "matplotlib==3.10.0" "torch==2.7.1" "transformers==4.52.3" \
    "scikit-learn==1.6.0" "mlflow==2.19.0" "ipywidgets==8.1.3"
