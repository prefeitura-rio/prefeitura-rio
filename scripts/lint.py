# -*- coding: utf-8 -*-
import subprocess
from glob import glob


def main():
    """
    Lint all python files in the project.
    """
    files = glob("**/*.py", recursive=True)
    subprocess.run(["flake8", *files])
