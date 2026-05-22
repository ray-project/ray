# Setup Guide

This guide explains how to set up the Ray clone repository for personal use and development.

## Prerequisites

To build and run Ray from source, you will need the following tools:

- **Python**: Version 3.10 to 3.14 is supported.
- **Bazel**: The build system used for the C++ core.
- **C++ Compiler**: `gcc` or `clang` on Linux/macOS, `MSVC` on Windows.
- **JDK**: (Optional) Required if you plan to work on the Java SDK.
- **Node.js & NPM**: (Optional) Required for building the dashboard.

## Initial Repository Setup

1. **Clone the repository**:
   ```bash
   git clone <your-repo-url>
   cd ray-clone
   ```

2. **Install Git Hooks**:
   We use git hooks to ensure code quality (linting, etc.).
   ```bash
   ./setup_hooks.sh
   ```

3. **Create a virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -U pip
   ```

## Building Ray

### Python Development (Editable Install)

The easiest way to develop Ray's Python SDK is to perform an editable install. This will automatically trigger Bazel to build the C++ core.

```bash
cd python
pip install -e .
```

If you only want to install dependencies without building the core (e.g., for simple Python-only changes), you can use:
```bash
pip install -r requirements.txt
```

### Building the C++ Core with Bazel

You can also build specific components directly using Bazel:

```bash
# Build the entire Ray package
bazel build //:ray_pkg

# Build just the Raylet (C++ component)
bazel build //src/ray/raylet:raylet
```

### Building Docker Images

If you prefer to work in a containerized environment, use the provided scripts:

```bash
./build-docker.sh
```

## Verifying the Setup

After installation, verify that Ray is working correctly:

```python
import ray
ray.init()

@ray.remote
def hello():
    return "Hello from Ray!"

print(ray.get(hello.remote()))
```
