# Cython in Ray

To get started, run the following from directory ```$RAY_HOME/examples/cython```:
bash
```
pip install scipy # For BLAS example
python setup.py develop
python cython_main.py --help
```

You can import the ```cython_examples``` module from a Python script or interpreter.

## Notes
- You **must** include the following two lines at the top of any ```*.pyx``` file:
```python
#!python
# cython: embedsignature=True, binding=True
```
- You cannot decorate Cython functions within a ```*.pyx``` file (there are ways around this, but creates a leaky abstraction between Cython and Python that would be very challenging to support generally). Instead, prefer the following in your Python code:
```
some_cython_func = ray.remote(some_cython_module.some_cython_func)
```
- You cannot transfer memory buffers to a remote function (see ```example8```, which currently fails); your remote function must return a value
- Have a look at ```cython_main.py```, ```cython_simple.pyx```, and ```setup.py``` for examples of how to call, define, and build Cython code, respectively. The Cython [documentation](http://cython.readthedocs.io/) is also very helpful.
- Several limitations come from Cython's own [unsupported](https://github.com/cython/cython/wiki/Unsupported) Python features.

## License
We include some code examples from the [BrainIAK](https://github.com/brainiak/brainiak) package. There are licenses associated with those examples, which may (or may not!) cause issue. We can remove prior to any merge.
