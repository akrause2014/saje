from __future__ import print_function
from contextlib import contextmanager
import os

@contextmanager
def cd(path):
    orig_path = os.getcwd()
    os.chdir(path)
    yield
    os.chdir(orig_path)
