# Use Python 3.10 base image
FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Install the 'uv' CLI
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Make sure 'uv' is on PATH
ENV PATH="/root/.local/bin:${PATH}"

# Set working directory
WORKDIR /app

# Copy and install only requirements first (caching)
COPY requirements.txt .
RUN uv pip install --system -r requirements.txt

# Now copy everything from the current directory into /app
COPY . .

# Run the server
CMD ["uv", "run", "weather.py"]
