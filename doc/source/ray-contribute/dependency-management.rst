======================
Managing Ray Dependencies
======================

This guide explains how Ray manages its Python dependencies and how to properly 
update them when contributing to the project. Ray uses a monolithic dependency 
management system that requires specific tools and processes for compilation.

Overview
--------

Ray uses a unique approach to dependency management that differs from typical 
Python projects:

- **Monolithic requirements**: All dependencies for Ray and its optional components 
  (Tune, Serve, Train, Data, RLlib, etc.) are compiled into a single 
  ``requirements_compiled.txt`` file
- **Constraint-based installation**: The compiled file serves as a pip constraint 
  file (``-c``) rather than a requirements file (``-r``)
- **Platform-specific compilation**: Dependencies must be compiled on specific 
  platforms to ensure compatibility

Understanding Ray's Dependency Structure
---------------------------------------

Source Requirements
^^^^^^^^^^^^^^^^^^^

Ray's dependencies are organized in separate files under ``python/requirements/``:

.. code-block:: text

    python/requirements/
    ├── requirements.txt              # Core Ray dependencies
    ├── test-requirements.txt         # Testing dependencies
    ├── lint-requirements.txt         # Linting and formatting tools
    ├── ml/
    │   ├── core-requirements.txt     # Core ML dependencies
    │   ├── tune-requirements.txt     # Ray Tune dependencies
    │   ├── train-requirements.txt    # Ray Train dependencies
    │   ├── data-requirements.txt     # Ray Data dependencies
    │   └── ...                       # Other ML component requirements
    └── cloud-requirements.txt        # Cloud provider dependencies

These source files define the actual dependency versions that get compiled into 
the monolithic file.

Compiled Requirements
^^^^^^^^^^^^^^^^^^^^^

The compiled requirements live in:

- ``python/requirements_compiled.txt`` - The main compiled file used in CI/CD
- ``python/requirements_compiled_py3.X.txt`` - Python version-specific compiled files

The compiled file contains **all** dependencies from all components, making it a 
comprehensive constraint file that ensures compatible versions across Ray's ecosystem.

Why This Approach?
^^^^^^^^^^^^^^^^^^

Ray uses this monolithic approach for several reasons:

1. **Dependency conflict resolution**: Prevents version conflicts between different 
   Ray components
2. **Reproducible builds**: Ensures consistent dependency versions across all 
   environments
3. **CI efficiency**: Single constraint file simplifies CI/CD pipeline dependency 
   management
4. **Integration testing**: Validates that all components work together with the 
   same dependency versions

Tool Requirements
-----------------

Ray's dependency compilation requires specific tool versions due to compatibility 
constraints:

.. warning::
   Using incorrect tool versions will cause compilation failures. Always use the 
   exact versions specified below.

Required Tools
^^^^^^^^^^^^^^

- **Python 3.11**: Required for Ray's compilation process
- **pip 25.2**: Specific version required for compatibility
- **pip-tools 7.4.1**: Exact version needed to avoid ``allow_all_prereleases`` errors

Installing Required Tools
^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

    # Ensure you have Python 3.11
    python --version  # Should show Python 3.11.x
    
    # Install exact tool versions
    pip install pip==25.2 pip-tools==7.4.1

Platform Requirements
---------------------

.. important::
   Ray's dependency compilation **only works on AMD64 platforms**. ARM64/M1 Macs 
   cannot compile Ray dependencies due to platform-specific constraints.

Supported Platforms
^^^^^^^^^^^^^^^^^^^

- ✅ Linux AMD64 (including CI environments)
- ✅ macOS Intel (x86_64) 
- ❌ macOS ARM64 (M1/M2/M3 Macs)
- ❌ Windows (dependencies typically compiled on Linux)

Alternative Solutions for ARM64
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you're on an ARM64 Mac and need to update dependencies:

1. **Use a cloud instance**: Spin up an AMD64 Linux instance (e.g., AWS EC2, Google Cloud)
2. **Use university HPC**: Many universities provide AMD64 Linux systems
3. **Use Docker with emulation**: Run an AMD64 Linux container (slower but works)
4. **Ask for help**: Request dependency compilation on the Ray Slack/Discord

How to Update Dependencies
--------------------------

Step 1: Update Source Requirements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

First, update the appropriate source requirements file:

.. code-block:: bash

    # Example: Update ax-platform in Tune requirements
    vim python/requirements/ml/tune-requirements.txt
    
    # Change version as needed
    # ax-platform==0.3.2  →  ax-platform==1.2.1

Step 2: Set Up Compilation Environment  
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Ensure you're on a supported platform with the correct tools:

.. code-block:: bash

    # Verify platform (should show x86_64/AMD64)
    uname -m
    
    # Verify Python version
    python --version  # Must be 3.11.x
    
    # Install exact tool versions
    pip install pip==25.2 pip-tools==7.4.1

Step 3: Compile Dependencies
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use Ray's official compilation script:

.. code-block:: bash

    # Navigate to Ray root directory
    cd /path/to/ray
    
    # Run Ray's official dependency compilation
    # This script handles all the complexity for you
    ci/ci.sh compile_pip_dependencies

This script:

- Compiles all requirement files into the monolithic format
- Handles platform-specific constraints
- Resolves dependency conflicts
- Generates both the main file and Python-specific variants

Step 4: Verify Compilation
^^^^^^^^^^^^^^^^^^^^^^^^^^

Check that your changes were applied correctly:

.. code-block:: bash

    # Verify your dependency update is in the compiled file
    grep "your-package" python/requirements_compiled.txt
    
    # Check for any obvious issues
    head -20 python/requirements_compiled.txt

The compiled file should contain your updated package with a compatible version.

Step 5: Test Locally
^^^^^^^^^^^^^^^^^^^^^

Test that Ray can be installed with your updated dependencies:

.. code-block:: bash

    # Create a fresh virtual environment
    python -m venv test_deps
    source test_deps/bin/activate
    
    # Install Ray with your compiled constraints
    pip install -c python/requirements_compiled.txt ray[default]
    
    # Test basic functionality
    python -c "import ray; ray.init(); print('Success!')"

Alternative: Manual Compilation (Advanced)
------------------------------------------

.. warning::
   Manual compilation is complex and error-prone. Use Ray's official script 
   whenever possible. This section is for understanding the process.

If you need to manually compile dependencies (e.g., for debugging), here's the 
process:

.. code-block:: bash

    # Navigate to python directory
    cd python/
    
    # Compile dependencies manually
    pip-compile --allow-unsafe --strip-extras \
        requirements.txt \
        requirements/test-requirements.txt \
        requirements/ml/core-requirements.txt \
        requirements/ml/tune-requirements.txt \
        # ... add all other requirement files ...
        --output-file requirements_compiled.txt

Common Issues and Troubleshooting
----------------------------------

ARM64 Platform Error
^^^^^^^^^^^^^^^^^^^^^

**Error**: "This platform is not supported for dependency compilation"

**Solution**: Use an AMD64 platform or Ray's official compilation script which 
checks platform compatibility.

pip-tools Version Error
^^^^^^^^^^^^^^^^^^^^^^^

**Error**: ``AttributeError: 'Command' object has no attribute 'allow_all_prereleases'``

**Solution**: Install the exact pip-tools version:

.. code-block:: bash

    pip install pip-tools==7.4.1

Constraint vs Requirements Error
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Error**: "Constraints cannot have extras"

**Solution**: This occurs when trying to use compiled dependencies as requirements 
instead of constraints. Always use the ``-c`` flag:

.. code-block:: bash

    # Correct usage
    pip install -c python/requirements_compiled.txt ray[default]
    
    # Incorrect usage (causes error)
    pip install -r python/requirements_compiled.txt

Dependency Conflicts
^^^^^^^^^^^^^^^^^^^^

**Error**: Conflicting dependency versions during compilation

**Solution**: 

1. Check if multiple requirement files specify different versions of the same package
2. Ensure you're compiling all requirement files together (not individually)
3. Consider if the new dependency version is compatible with Ray's ecosystem

CI Failures After Update
^^^^^^^^^^^^^^^^^^^^^^^^^

If CI fails after your dependency update:

1. **Check BuildKite logs**: Look for specific installation or import errors
2. **Verify compilation**: Ensure the compiled file contains your expected versions
3. **Test compatibility**: Make sure the new dependency version works with Ray's code
4. **Check transitive dependencies**: Your update might have pulled in incompatible versions

Best Practices
--------------

Version Selection
^^^^^^^^^^^^^^^^^

When updating dependencies:

- **Use specific versions**: Avoid ranges (``>=1.0``) in source requirements
- **Test compatibility**: Verify new versions work with Ray's codebase
- **Check security**: Ensure new versions don't introduce vulnerabilities
- **Review changelogs**: Understand what changes in the new version

Testing Process
^^^^^^^^^^^^^^^

Before submitting a PR:

1. **Local testing**: Verify Ray installs and runs with your changes
2. **Component testing**: Test the specific Ray component that uses the dependency
3. **Integration testing**: Run relevant test suites
4. **CI validation**: Wait for CI to pass before requesting review

Documentation
^^^^^^^^^^^^^

When updating dependencies:

- **Add comments**: Explain why a specific version was chosen
- **Document breaking changes**: Note any API changes that affect Ray
- **Update any affected documentation**: If the dependency change affects user APIs

Contributing Dependency Updates
-------------------------------

When contributing dependency updates to Ray:

1. **Create an issue first**: Discuss major dependency updates with maintainers
2. **Follow the process**: Use the steps outlined above
3. **Provide context**: Explain why the update is needed (security, features, bugs)
4. **Test thoroughly**: Ensure your changes don't break existing functionality
5. **Update documentation**: If the change affects user-facing APIs

Example: Complete Workflow
--------------------------

Here's a complete example of updating the ``ax-platform`` dependency:

.. code-block:: bash

    # 1. Fork and clone Ray repository
    git clone https://github.com/YOUR-USERNAME/ray.git
    cd ray
    
    # 2. Create a branch
    git checkout -b update-ax-platform
    
    # 3. Update source requirements
    vim python/requirements/ml/tune-requirements.txt
    # Change: ax-platform==0.3.2 → ax-platform==1.2.1
    
    # 4. Verify you're on AMD64 platform
    uname -m  # Should show x86_64
    
    # 5. Set up tools
    pip install pip==25.2 pip-tools==7.4.1
    
    # 6. Compile dependencies
    ci/ci.sh compile_pip_dependencies
    
    # 7. Verify the update
    grep "ax-platform" python/requirements_compiled.txt
    # Should show: ax-platform==1.2.1
    
    # 8. Test locally
    python -m venv test_env
    source test_env/bin/activate
    pip install -c python/requirements_compiled.txt ray[tune]
    
    # 9. Run relevant tests
    python -m pytest python/ray/tune/tests/ -v
    
    # 10. Commit and push
    git add python/requirements/ml/tune-requirements.txt python/requirements_compiled.txt
    git commit -m "Update ax-platform to 1.2.1 for improved Bayesian optimization"
    git push origin update-ax-platform

Further Resources
-----------------

- **Ray Development Guide**: :doc:`development` for setting up development environment
- **Testing Guide**: :doc:`getting-involved` for information about testing Ray
- **CI Documentation**: Understanding Ray's CI system for dependency validation
- **Community Support**: Ray Slack/Discord for help with dependency issues

.. note::
   This guide covers the current dependency management system as of 2026. The 
   process may evolve, so always check the latest documentation and CI scripts 
   for any updates.