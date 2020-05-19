#    Copyright 2020 Matthew Wigginton Conway

#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#        http://www.apache.org/licenses/LICENSE-2.0

#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

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