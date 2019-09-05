Ray Projects (Experimental)
===========================

Ray projects make it easy to package a Ray application so it can be
rerun later in the same environment. They allow for the sharing and
reliable reuse of existing code.

Quick start (CLI)
-----------------

.. code-block:: bash

    # Creates a project in the current directory. It will create a
    # project.yaml defining the code and environment and a cluster.yaml
    # describing the cluster configuration. Both will be created in the
    # .rayproject subdirectory of the current directory.
    $ ray project create <project-name>

    # Create a new session from the given project.  Launch a cluster and run
    # the command, which must be specified in the project.yaml file. If no
    # command is specified, the "default" command in .rayproject/project.yaml
    # will be used. Alternatively, use --shell to run a raw shell command.
    $ ray session start <command-name> [arguments] [--shell]

    # Open a console for the given session.
    $ ray session attach

    # Stop the given session and terminate all of its worker nodes.
    $ ray session stop

Examples
--------
See `the readme <https://github.com/ray-project/ray/blob/master/python/ray/projects/examples/README.md>`__
for instructions on how to run these examples:

- `Open Tacotron <https://github.com/ray-project/ray/blob/master/python/ray/projects/examples/open-tacotron/.rayproject/project.yaml>`__:
  A TensorFlow implementation of Google's Tacotron speech synthesis with pre-trained model (unofficial)
- `PyTorch Transformers <https://github.com/ray-project/ray/blob/master/python/ray/projects/examples/pytorch-transformers/.rayproject/project.yaml>`__:
  A library of state-of-the-art pretrained models for Natural Language Processing (NLP)

Project file format (project.yaml)
----------------------------------

A project file contains everything required to run a project.
This includes a cluster configuration, the environment and dependencies
for the application, and the specific inputs used to run the project.

Here is an example for a minimal project format:

.. code-block:: yaml

    name: test-project
    description: "This is a simple test project"
    repo: https://github.com/ray-project/ray

    # Cluster to be instantiated by default when starting the project.
    cluster: .rayproject/cluster.yaml

    # Commands/information to build the environment, once the cluster is
    # instantiated. This can include the versions of python libraries etc.
    # It can be specified as a Python requirements.txt, a conda environment,
    # a Dockerfile, or a shell script to run to set up the libraries.
    environment:
      requirements: requirements.txt

    # List of commands that can be executed once the cluster is instantiated
    # and the environment is set up.
    # A command can also specify a cluster that overwrites the default cluster.
    commands:
      - name: default
        command: python default.py
        help: "The command that will be executed if no command name is specified"
      - name: test
        command: python test.py --param1={{param1}} --param2={{param2}}
        help: "A test command"
        params:
          - name: "param1"
            help: "The first parameter"
            # The following line indicates possible values this parameter can take.
            choices: ["1", "2"]
          - name: "param2"
            help: "The second parameter"

Project files have to adhere to the following schema:

.. jsonschema:: ../../python/ray/projects/schema.json

Cluster file format (cluster.yaml)
----------------------------------

This is the same as for the autoscaler, see
`Cluster Launch page <autoscaling.html>`_.
