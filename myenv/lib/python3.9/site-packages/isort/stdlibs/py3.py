from . import py35, py36, py37, py38, py39, py310

stdlib = py35.stdlib | py36.stdlib | py37.stdlib | py38.stdlib | py39.stdlib | py310.stdlib
