import os
import re
from typing import List, Set

from ci.ray_ci.doc.api import (
    API,
    _SPHINX_AUTOSUMMARY_HEADER,
    _SPHINX_AUTOCLASS_HEADER,
)


_SPHINX_CURRENTMODULE_HEADER = ".. currentmodule::"
_SPHINX_TOCTREE_HEADER = ".. toctree::"
_SPHINX_INCLUDE_HEADER = ".. include::"


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
        self._autodoc_rsts = None
        self._apis = None

    def get_apis(self) -> List[API]:
        self.walk()
        return self._apis or []

    def walk(self) -> None:
        if self._apis is not None:
            # already walk
            return
        rsts = self._get_autodoc_rsts()
        self._apis = []
        for rst in rsts:
            self._apis.extend(self._parse_autodoc_rst(rst))

    def _get_autodoc_rsts(self) -> Set[str]:
        """
        Parse the list of rst declared in the head_rst_file, for example:

        .. toctree::
            :option

            area_01.rst
            area_02.rst
        """
        if self._autodoc_rsts is not None:
            return self._autodoc_rsts

        self._autodoc_rsts = {self._head_rst_file}
        visited = set()
        while len(self._autodoc_rsts) > len(visited):
            not_visited = self._autodoc_rsts - visited
            for rst in not_visited:
                visited.add(rst)
                self._autodoc_rsts.update(self._get_autodoc_rsts_from_single_file(rst))

        return self._autodoc_rsts

    def _get_autodoc_rsts_from_single_file(self, rst_file: str) -> Set[str]:
        rsts = set()
        dir = os.path.dirname(rst_file)
        with open(rst_file, "r") as f:
            line = f.readline()
            while line:
                # look for the include block
                if line.strip().startswith(_SPHINX_INCLUDE_HEADER):
                    rsts.add(os.path.join(dir, line.strip().split("::")[1].strip()))
                    line = f.readline()
                    continue

                # look for the toctree block
                if not line.strip() == _SPHINX_TOCTREE_HEADER:
                    line = f.readline()
                    continue

                # parse the toctree block
                line = f.readline()
                while line:
                    if line.strip() and not re.match(r"\s", line):
                        # end of toctree, \s means empty space, this line is checking if
                        # the line is not empty and not starting with empty space
                        break
                    if line.strip().endswith(".rst"):
                        rsts.add(os.path.join(dir, line.strip()))
                    line = f.readline()

        return rsts

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
        module = None
        with open(rst_file, "r") as f:
            line = f.readline()
            while line:
                # parse currentmodule block
                if line.startswith(_SPHINX_CURRENTMODULE_HEADER):
                    module = line[len(_SPHINX_CURRENTMODULE_HEADER) :].strip()

                # parse autoclass block
                if line.startswith(_SPHINX_AUTOCLASS_HEADER):
                    apis.append(API.from_autoclass(line, module))

                # parse autosummary block
                if line.startswith(_SPHINX_AUTOSUMMARY_HEADER):
                    doc = line
                    line = f.readline()
                    # collect lines until the end of the autosummary block
                    while line:
                        doc += line
                        if line.strip() and not re.match(r"\s", line):
                            # end of autosummary, \s means empty space, this line is
                            # checking if the line is not empty and not starting with
                            # empty space
                            break
                        line = f.readline()

                    apis.extend(API.from_autosummary(doc, module))
                    continue

                line = f.readline()

        return [api for api in apis if api]
