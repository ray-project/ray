
from ray import serve
from ray.serve.experimental.models import A, B, C, D


@serve.deployment
A:


######################################

# TODO:
"""
1) Map imported class to
"""

# Milestones
"""
1) Deploy A-B-C-D and just let it run, path of minimal resistence.
 - No upgrade or resize
 - No fancy config

2) Deploy and configure via cli + yaml
 - Yaml can just translate to inputs to @serve.deployment without changing API or implementation

3) Resize single node

4) Upgrade single node

5) Bonus .. benchmark and optimize a little ?
"""

######################################

# In-flight questions:
"""
1) code -> DAG -> driver needs to have local execution ability

"""

######################################

# Conclusions:

"""
Each pipeline node is a serve deployment, use existing deployments API and mechanism.

Infer class names in code is great, but needs to take care of one class name with different instantiations.
  - ClassName + Unique id (hash of code + config ?)

"""
