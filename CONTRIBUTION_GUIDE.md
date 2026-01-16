# Ray Contribution Guide

## What is Ray?

**Ray** is a unified framework for scaling AI and Python applications. It consists of:

### Core Components:
- **Ray Core**: Distributed runtime with key abstractions:
  - **Tasks**: Stateless functions executed in the cluster
  - **Actors**: Stateful worker processes created in the cluster
  - **Objects**: Immutable values accessible across the cluster

### AI Libraries:
- **Ray Data**: Scalable Datasets for ML
- **Ray Train**: Distributed Training
- **Ray Tune**: Scalable Hyperparameter Tuning
- **RLlib**: Scalable Reinforcement Learning
- **Ray Serve**: Scalable and Programmable Serving

## Repository Structure

```
ray/
├── python/ray/          # Main Python codebase
│   ├── core/            # Core distributed runtime
│   ├── data/            # Ray Data library
│   ├── train/           # Ray Train library
│   ├── tune/            # Ray Tune library
│   ├── rllib/           # RLlib reinforcement learning
│   ├── serve/           # Ray Serve model serving
│   ├── autoscaler/      # Cluster autoscaling
│   ├── dashboard/       # Web dashboard (React/TypeScript)
│   └── _private/        # Internal implementation details
├── src/ray/             # C++ core implementation
├── java/                # Java API bindings
├── cpp/                 # C++ API
├── bazel/               # Bazel build configuration
├── doc/                 # Documentation
└── ci/                  # CI/CD configuration
```

## Technology Stack

- **Languages**: Python (primary), C++, Java, TypeScript/React
- **Build System**: Bazel (for C++/Java), setuptools (for Python)
- **Testing**: pytest
- **Python Versions**: 3.10, 3.11, 3.12, 3.13

## Getting Started with Contributions

### 1. Fork and Clone

```bash
# Fork the repository on GitHub, then:
git clone https://github.com/YOUR_USERNAME/ray.git
cd ray
git remote add upstream https://github.com/ray-project/ray.git
```

### 2. Set Up Development Environment

#### Option A: Python-Only Development (Recommended for beginners)

If you're only editing Python files (RLlib, Tune, Train, Serve, Data, Autoscaler):

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install latest Ray wheel
pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp310-cp310-manylinux2014_x86_64.whl

# Link your local code to installed package
python python/ray/setup-dev.py
```

This creates symlinks so your local changes are immediately reflected.

#### Option B: Full Build (For Core/C++ changes)

Requires Bazel and takes longer. See [Building Ray from Source](https://docs.ray.io/en/latest/ray-contribute/development.html).

### 3. Find Issues to Work On

Use GitHub labels to find suitable work:

- **`good-first-issue`**: Small issues perfect for new contributors
- **`contribution-welcome`**: Impactful issues prioritized for review
- **By Component**: `core`, `data`, `train`, `tune`, `serve`, `rllib`
- **By Type**: `bug`, `enhancement`, `docs`

**Links:**
- [Good First Issues](https://github.com/ray-project/ray/issues?q=is%3Aissue+is%3Aopen+label%3A%22good-first-issue%22)
- [Contribution Welcome](https://github.com/ray-project/ray/issues?q=is%3Aissue+is%3Aopen+label%3A%22contribution-welcome%22)

### 4. Development Workflow

```bash
# Create a feature branch
git checkout -b your-feature-branch

# Make your changes
# ... edit files ...

# Run tests (if applicable)
pytest python/ray/tests/path/to/test_file.py

# Commit your changes
git add .
git commit -m "Description of your changes"

# Push to your fork
git push origin your-feature-branch

# Create a Pull Request on GitHub
```

### 5. Testing Requirements

- **Minimum 85% test coverage** for new code
- Unit tests for every function/method
- Integration tests for component interactions
- All tests must pass before PR merge

Run tests:
```bash
# Run specific test file
pytest python/ray/tests/path/to/test_file.py

# Run with coverage
pytest --cov=ray python/ray/tests/path/to/test_file.py
```

### 6. Code Quality Standards

- **Zero linting warnings** (uses ruff, pylint)
- Follow existing code patterns and conventions
- Comprehensive error handling and logging
- Clear, readable code over clever tricks
- Update documentation with every change

### 7. PR Review Process

1. Create PR with clear description
2. Assign a reviewer (if you're in ray-project org)
3. Address review comments
4. Ensure all CI checks pass
5. Add `test-ok` label when build succeeds
6. Committer merges once approved

## Key Areas for Contribution

### Beginner-Friendly Areas

1. **Documentation** (`docs` label)
   - Fix typos, improve clarity
   - Add examples and tutorials
   - Update API documentation

2. **Python Libraries** (Tune, Train, Serve, Data)
   - Most changes don't require C++ compilation
   - Use Python-only development setup
   - Good test coverage examples

3. **Bug Fixes** (`bug` label)
   - Start with small, isolated bugs
   - Good way to learn codebase structure

### Intermediate Areas

1. **Ray Core** (`core` label)
   - Distributed runtime improvements
   - May require C++ knowledge
   - Performance optimizations

2. **RLlib** (`rllib` label)
   - Reinforcement learning algorithms
   - Algorithm improvements
   - New environments support

3. **Dashboard** (`dashboard` label)
   - React/TypeScript frontend
   - UI improvements
   - New visualizations

## Project Roadmap

The Ray project roadmap is tracked in several places:

### Primary Roadmap Sources

1. **GitHub Roadmap Issues**
   - Quarterly roadmap issues (e.g., "Roadmap Q3 2025")
   - Component-specific roadmaps (e.g., "Roadmap for Data and Serve LLM APIs")
   - Search for roadmap issues: https://github.com/ray-project/ray/issues?q=is%3Aissue+roadmap

2. **Ray Enhancement Proposals (REP)**
   - Major features and architectural changes are proposed via REPs
   - Repository: https://github.com/ray-project/enhancements
   - Browse accepted proposals to understand future direction

3. **Ray Discuss Forum**
   - Roadmap discussions and announcements
   - https://discuss.ray.io (search for "roadmap")

4. **Component-Specific Roadmaps**
   - Dashboard roadmap: https://github.com/ray-project/ray/issues/30097
   - Feature-specific roadmaps in relevant GitHub issues

### Current Roadmap Highlights (Q3 2025)

Based on recent roadmap discussions, key focus areas include:

- **Ray Core**: System reliability, fault tolerance, label-based scheduling, GPU optimizations
- **Ray Data**: Cluster failure resilience, better sampling/caching, new connectors (Iceberg, Unity Catalog)
- **LLMs/Serve/Data**: Large model scaling, request routing improvements, heterogeneous accelerators
- **Ray Train**: Train V2 API finalization, asynchronous checkpointing
- **Observability**: Unified event export APIs, tracing improvements
- **RLlib**: RL V2, algorithm combinability improvements

**Note**: Roadmap priorities change over time. Check the latest roadmap issues for current priorities.

## Resources

- **Documentation**: https://docs.ray.io
- **Discourse Forum**: https://discuss.ray.io (for questions)
- **GitHub Issues**: https://github.com/ray-project/ray/issues
- **Ray Enhancement Proposals**: https://github.com/ray-project/enhancements
- **Slack**: https://www.ray.io/join-slack
- **StackOverflow**: Tag questions with `ray`

## Tips for Success

1. **Start Small**: Pick `good-first-issue` labels
2. **Ask Questions**: Use Discourse or Slack for help
3. **Get Early Feedback**: Discuss substantial changes before implementing
4. **Write Tests**: Every change needs tests
5. **Follow Patterns**: Study existing code before making changes
6. **Update Docs**: Keep documentation in sync with code changes

## Next Steps

1. ✅ Set up Python-only development environment
2. ✅ Browse `good-first-issue` labels on GitHub
3. ✅ Pick an issue that interests you
4. ✅ Comment on the issue to claim it
5. ✅ Make your changes and submit a PR
6. ✅ Engage with reviewers and iterate

Good luck with your contributions! 🚀

