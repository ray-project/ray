import pickle
from sphinx.project import Project
import os
import time
from datetime import datetime, timezone
env = None
RAY_DOC_DIR = "/Users/kevin/ray/doc"


# with open(os.path.join(RAY_DOC_DIR, "_build/doctrees/environment.pickle"), "rb") as f:
#     env = pickle.load(f)
#     env.srcdir = os.path.join(RAY_DOC_DIR, "source")
#     env.doctreedir = os.path.join(RAY_DOC_DIR, "_build/doctrees")
#     env.project.srcdir = os.path.join(RAY_DOC_DIR, "source")
#     p = Project(os.path.join(RAY_DOC_DIR, "source"), {'.rst': 'restructuredtext', '.md': 'myst-nb', '.ipynb': 'myst-nb'})
#     p.discover()
#     env.project = p

#     for doc, val in env.all_docs.items():
#         env.all_docs[doc] = int(time.time()) * 1000000

# with open(os.path.join(RAY_DOC_DIR, "_build/doctrees/environment.pickle"), "wb+") as f:
#     pickle.dump(env, f, pickle.HIGHEST_PROTOCOL)
with open(os.path.join(RAY_DOC_DIR, "_build/doctrees/environment.pickle"), "rb") as f:
    env = pickle.load(f)
    for doc, val in env.all_docs.items():
        if "save_to_path" in doc:
            print(doc, val)
    