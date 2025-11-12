# -*- coding: utf-8 -*-
from setuptools import find_packages, setup

import os
from shutil import rmtree

if os.path.exists("computer_use_server/build"):
    rmtree("computer_use_server/build")
if os.path.exists("computer_use_server/computer_use.egg-info"):
    rmtree("computer_use_server/computer_use.egg-info")

with open("computer_use_server/requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="computer-use",
    version="0.0.6",
    description="computer use demo",
    packages=find_packages(),
    install_requires=requirements,
)
