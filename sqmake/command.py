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

from logging import getLogger
import subprocess

LOG = getLogger(__name__)

class Command(object):
    def __init__ (self, fn, code):
        'handle abstraction of file or direct code'
        if code is None:
            with open(fn) as inf:
                self.code = inf.read()
        else:
            self.code = code

    def run (self, engine):
        pass # abstract

    @staticmethod
    def from_yaml (yaml):
        'Build from a deserialized YAML representation'
        fn = yaml['file'] if 'file' in yaml else None
        code = yaml['code'] if 'code' in yaml else None
        if yaml['type'] == 'sql':
            return SqlCommand(fn, code)
        elif yaml['type'] == 'sh':
            return ShellCommand(fn, code)
        else:
            raise ValueError(f'Unknown type {yaml["type"]}')

class SqlCommand(Command):
    def __init__ (self, fn, code):
        super().__init__(fn, code)

    def run (self, engine):
        LOG.info(f'sql> {self.code}')
        with engine.begin() as con:
            con.execute(self.code)

class ShellCommand(Command):
    def __init__ (self, fn, code):
        super().__init__(fn, code)

    def run (self, engine):
        LOG.info(f'sh> {self.code}')
        # TODO how to handle working directory for this process?
        process = subprocess.Popen(self.code, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        while (retcode := process.poll()) is None:
            try:
                stdout, stderr = process.communicate(timeout=1)
            except subprocess.TimeoutExpired:
                pass # poll again
            else:
                LOG.info(stdout)
                LOG.info(stderr)
        
        if retcode != 0:
            raise CommandFailedError('Command terminated with non-zero return code {}'.format(retcode))

class CommandFailedError(Exception):
    pass
