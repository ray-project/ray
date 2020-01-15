Ray Projects
============

To run these example projects, we first have to make sure the full
repository is checked out into the project directory.

Open Tacotron
-------------

```shell
cd open-tacotron
# Check out the original repository
git init
git remote add origin https://github.com/keithito/tacotron.git
git fetch
git checkout -t origin/master

# Serve the model
ray session start serve

# Terminate the session
ray session stop
```

PyTorch Transformers
--------------------

```shell
cd python-transformers
# Check out the original repository
git init
git remote add origin https://github.com/huggingface/pytorch-transformers.git
git fetch
git checkout -t origin/master

# Now we can start the training
ray session start train --dataset SST-2

# Terminate the session
ray session stop
```
