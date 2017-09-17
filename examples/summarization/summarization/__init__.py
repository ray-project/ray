from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gzip
import os.path
import shutil
import urllib.request

from .summarization import SummarizationEnv, Word2VecPreprocessor
from .similarity_summarization import SimilaritySummarizationEnv

from gym.envs.registration import register
from ray.rllib.models.catalog import ModelCatalog

if not os.path.isfile("/tmp/wikipedia-summaries.csv"):
    print("Downloading wikipedia-summaries.csv...")
    response = urllib.request.urlopen("https://people.eecs.berkeley.edu/~pcmoritz/data/wikipedia-summaries.csv.gz", timeout=5)
    source = gzip.GzipFile(fileobj=response)
    with open("/tmp/wikipedia-summaries.csv", "wb") as target:
        shutil.copyfileobj(source, target)

register(id='SimpleSummarization-v0', entry_point='summarization:SummarizationEnv', kwargs={"filepath": "/tmp/wikipedia-summaries.csv"}, nondeterministic=False)

ModelCatalog.register_preprocessor('SimpleSummarization-v0', Word2VecPreprocessor)
