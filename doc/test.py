import pickle
from sphinx.project import Project
import os
import time
from datetime import datetime, timezone
env = None
RAY_DOC_DIR = "/Users/kevin/test2/ray/doc"


with open(os.path.join(RAY_DOC_DIR, "_build/doctrees/environment.pickle"), "rb") as f:
    env = pickle.load(f)
    env.srcdir = os.path.join(RAY_DOC_DIR, "source")
    env.doctreedir = os.path.join(RAY_DOC_DIR, "_build/doctrees")
    env.project.srcdir = os.path.join(RAY_DOC_DIR, "source")
    p = Project(os.path.join(RAY_DOC_DIR, "source"), {'.rst': 'restructuredtext', '.md': 'myst-nb', '.ipynb': 'myst-nb'})
    p.discover()
    env.project = p

    for doc, val in env.all_docs.items():
        if "cluster/kubernetes/examples/ml-example" not in doc:
            env.all_docs[doc] = int(time.time()) * 1000000
    
    """
    doc/Makefile
    doc/mtime.py
    doc/source/_templates/main-sidebar.html
    doc/source/cluster/kubernetes/examples/ml-example.md
    doc/test.py
    doc/timestamp.py
    """
    for doc, dep_set in env.dependencies.items():
        site_packages = {d for d in dep_set if "site-packages" in d}
        for dep in site_packages:
            env.dependencies[doc].remove(dep)

with open(os.path.join(RAY_DOC_DIR, "_build/doctrees/environment.pickle"), "wb+") as f:
    pickle.dump(env, f, pickle.HIGHEST_PROTOCOL)
# with open(os.path.join(RAY_DOC_DIR, "_build/doctrees/environment.pickle"), "rb") as f:
#     env = pickle.load(f)
#     for doc, val in env.all_docs.items():
#         if "ray.data.DataIterator" in doc:
#             print(doc, val)

# with open(os.path.join(RAY_DOC_DIR, "_build/doctrees/environment.pickle"), "rb") as f:
#     env = pickle.load(f)
#     for doc, val in env.all_docs.items():
#         if "ml-example" in doc:
#             print(doc, val)
        