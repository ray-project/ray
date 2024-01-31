# HPU Unit Tests Documentation

## Overview

The new suite of unit tests is designed to validate the functionality of a Ray cluster's handling of HPU resources. The tests cover a range of functionalities, including resource specification, resource release upon actor deletion, correct HPU assignment, behavior of blocking tasks, and configuration of visible devices.

## Environment Setup

To ensure the HPU unit tests run as expected, it's essential to configure the testing environment according to the specifications that have been verified. The unit tests have been designed and tested using the Docker image `vault.habana.ai/gaudi-docker/1.13.0/ubuntu22.04/habanalabs/pytorch-installer-2.1.0:latest`, which is based on Ubuntu 22.04 and includes Python 3.10. Follow the steps below to prepare your environment:

Follow these commands to set up the environment:

1. **Prepare the Docker Environment**
    
    First, ensure that the drivers and container runtime are installed, if not, refer to this [guide](https://docs.habana.ai/en/latest/Installation_Guide/Bare_Metal_Fresh_OS.html?highlight=installer#run-using-containers). You can run `hl-smi` to verify your installation.

2. **Pull and Run the Docker**
   
    Start by pulling and running the specified Docker image. This image contains the necessary dependencies and the correct version of Python to execute the unit tests.

    ```bash
    docker pull vault.habana.ai/gaudi-docker/1.13.0/ubuntu22.04/habanalabs/pytorch-installer-2.1.0:latest

    docker run -it --runtime=habana -e HABANA_VISIBLE_DEVICES=all -e OMPI_MCA_btl_vader_single_copy_mechanism=none --cap-add=sys_nice --net=host --ipc=host vault.habana.ai/gaudi-docker/1.13.0/ubuntu22.04/habanalabs/pytorch-installer-2.1.0:latest
    ```

## Test Execution

1. Navigate to the Ray project directory:
   ```
   cd ray
   ```

2. Execute the unit tests using pytest:
   ```
   pytest python/ray/tests/hpu
   ```

## Test Descriptions

The HPU unit tests are executed using the pytest framework, which provides a clear and concise way to determine the outcome of each test. After running the tests, pytest will display the results in the terminal. Here's how to interpret the results:

- **Passed**: If a test has executed successfully without any errors, pytest will mark it as "PASSED". This indicates that the test has met all the assertions and conditions defined within it, and the functionality being tested is behaving as expected.

  Example output for a successful test:
  ```
  test_example.py::test_specific_functionality PASSED
  ```

- **Failed**: If a test does not meet the assertions or if an error occurs during its execution, pytest will mark it as "FAILED". This indicates an issue that needs to be addressed.

  Example output for a failed test:
  ```
  test_example.py::test_specific_functionality FAILED
  ```

### Basic Unit Tests

#### `test_decorator_args`

**Objective:** Verify the functionality of the `@ray.remote` decorator with HPU resource specifications.

#### `test_actor_deletion_with_hpus`

**Objective:** Confirm the release of HPU resources back to the system upon actor deletion.

#### `test_actor_hpus`

**Objective:** Check for correct HPU assignment to actors without overlap in a multi-node Ray cluster.

#### `test_blocking_actor_task`

**Objective:** Ensure that blocking actor methods do not release HPU resources prematurely.

#### `test_actor_habana_visible_devices`

**Objective:** Test the configuration of the `HABANA_VISIBLE_MODULES` environment variable within an actor.

### Advanced Unit Tests

#### `test_hpu_ids`

**Objective:** Verify the correct assignment of HPU IDs and the matching of `HABANA_VISIBLE_MODULES`.

#### `test_hpu_with_placement_group`

**Objective:** Ensure correct scheduling of actors using HPUs within a placement group.