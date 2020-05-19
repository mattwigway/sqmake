import contextlib
import os

# a context manager that allows changing directories within a with block
# http://kitchingroup.cheme.cmu.edu/blog/2013/06/16/Automatic-temporary-directory-changing/
@contextlib.contextmanager
def withdir (path):
    origpath = os.getcwd()
    os.chdir(path)
    try:
        yield # run code in with block
    finally:
        os.chdir(origpath)