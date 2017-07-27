import ipywidgets as widgets
import numpy as np
import os
import ray
import subprocess
import tempfile
import time

from IPython.display import display

class UI(object):

    def object_search_bar(self):
        object_search = widgets.Text(
        value="",
        placeholder="Object ID",
        description="Search for an object:",
        disabled=False
        )
        display(object_search)

        def handle_submit(sender):
            pp = pprint.PrettyPrinter()
            pp.pprint(ray.global_state.object_table(object_search.value))

        object_search.on_submit(handle_submit)
