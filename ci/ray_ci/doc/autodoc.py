import os
import re
from typing import List

from ci.ray_ci.doc.api import (
    API,
    SPHINX_AUTOSUMMARY_HEADER,
    SPHINX_AUTOCLASS_HEADER,
    SPHINX_CURRENTMODULE_HEADER,
)


class Autodoc:
    """
    Autodoc class represents the top level sphinx autodoc landing page and finds
    autodoc APIs that would be generated from sphinx from all sub-pages
    """

    def __init__(self, head_rst_file: str):
        """
        Args:
            head_rst_file: The path to the landing page rst file that contains the list
            of children rsts of the autodoc APIs
        """
        self._head_rst_file = head_rst_file
        self._autodoc_rsts = []
        self._apis = []

    def get_apis(self) -> List[API]:
        self.walk()
        return self._apis

    def walk(self) -> None:
        if self._apis:
            # already walk
            return
        rsts = self._get_autodoc_rsts()
        for rst in rsts:
            self._apis.extend(self._parse_autodoc_rst(rst))

    def _get_autodoc_rsts(self) -> List[str]:
        """
        Parse the list of rst declared in the head_rst_file, for exampe:

        .. toctree::

            area_01.rst
            area_02.rst
        """
        if self._autodoc_rsts:
            return self._autodoc_rsts

        dir = os.path.dirname(self._head_rst_file)
        with open(self._head_rst_file, "r") as f:
            for line in f:
                line = line.strip()
                if not line.endswith(".rst"):
                    continue

                self._autodoc_rsts.append(os.path.join(dir, line))

        return self._autodoc_rsts

    def _parse_autodoc_rst(self, rst_file: str) -> List[API]:
        """
        Parse the rst file to find the autodoc APIs. Example content of the rst file


        .. currentmodule:: mymodule

        .. autoclass:: myclass

        .. autosummary::

            myclass.myfunc_01
            myclass.myfunc_02
        """
        apis = []
        current_module = None
        current_line = "start"  # dummy non-empty value
        with open(rst_file, "r") as f:
            while current_line:
                current_line = current_line.strip()

                if current_line.startswith(SPHINX_CURRENTMODULE_HEADER):
                    current_module = current_line[
                        len(SPHINX_CURRENTMODULE_HEADER) :
                    ].strip()

                if current_line.startswith(SPHINX_AUTOCLASS_HEADER):
                    apis.append(API.from_autoclass(current_line, current_module))

                if current_line.startswith(SPHINX_AUTOSUMMARY_HEADER):
                    doc = current_line
                    line = f.readline()
                    # collect lines until the end of the autosummary block
                    while line:
                        if line.strip() and not re.match(r"\s", line):
                            # end of autosummary block if as_line does not start with
                            # whitespace
                            break
                        doc += line
                        line = f.readline()

                    apis.extend(API.from_autosummary(doc, current_module))
                    current_line = line
                else:
                    current_line = f.readline()

        return [api for api in apis if api]
