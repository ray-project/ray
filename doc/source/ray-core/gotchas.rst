.. _gotchas:

Ray Gotchas
===========

Ray sometimes has some aspects of its behavior that might catch 
users off guard. There may be sound arguments for these design choices. 

In particular, users think of Ray as running on their local machine, and 
while this is mostly true, this doesn't work. 

Environment variables are not passed from the driver to workers
---------------------------------------------------------------

**Issue**

If you set an environment variable at the command line, it is not passed to all the workers running in the cluster. 

**Example**

If you do ``FOO=bar python baz.py``

and then in ``baz.py`` there is 

.. code-block:: python

  @ray.remote
  def myfunc():
    myenv = os.environ.get('FOO'); 
    print(f'myenv is {myenv}'}


this will print ``myenv is None``

**Expected behavior**

Most people would expect (as if it was a single process on a single machine) that the environment variables would be the same in all workers. It won’t be.  

**Fix**

Use runtime environments to pass environment variables explicity. 

In the ``ray.init()`` call pass the environment variable, 
e.g. if you do 
``ray.init(runtime_env = {"env_vars": {"FOO": "bar"}})`` 
that should achieve the same goal.

Filenames work sometimes and not at other times
-----------------------------------------------

**Issue**

If you reference a file by name in a task or actor, 
it will sometimes work and sometimes fail. This is 
because if the task or actor runs on the head node 
of the cluster, it will work, but if the task or actor 
runs on another machine it won’t.

**Example**

Let’s say we do the following command:

.. code-block:: bash

	% touch /tmp/foo.txt


And I have this code: 

.. code-block:: python

  from os.path import exists

  ray.init()
  @ray.remote
  def checkFile():
    foo_exists = exists('/tmp/foo.txt')
    print(f'Foo exists? {foo_exists}')
  
  futures = []
  for _ in range(1000): 
    futures.append(checkFile.remote())

  ray.get(futures)


then you will get a mix of True and False. If 
``checkFile()`` runs on the head node, or we’re running 
locally it works. But if it runs on a worker node, it returns ``False``.

**Expected behavior**

Most people would expect this to either fail or succeed consistently. 
It’s the same code after all. 

**Fix**

- Use only shared paths for such applications -- e.g. if you are using a network file system you can use that, or the files can be on s3. 
- Do not rely on file path consistency.



Placement groups are not composable
-----------------------------------


**Issue**

If you have a task that is called from something that runs in a placement
group, the resources are never allocated and it hangs. 

**Example**

You are using Ray Tune which creates placement groups, and you want to 
apply it to an objective function, but that objective function makes use 
of Ray Tasks itself, e.g.

.. code-block:: python

  def objective(config):
    createTaskThatUsesResources()

  analysis = tune.run(objective, config=search_space)

This will hang forever. 

**Expected behavior**

The above executes and doesn’t hang. 

**Fix**

In the ``@ray.remote`` declaration of tasks 
called by ``createTaskThatUsesResources()`` , include a 
``placement_group=None``.