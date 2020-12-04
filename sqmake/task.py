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

import sqlalchemy as sq

from .command import Command
from .output import Output

class Task(object):
    'Represents a single task'
    def __init__ (self, name, commands, outputs, depends_on):
        self.name = name
        self.commands = commands
        self.outputs = outputs
        self.depends_on = depends_on

    def metatask (self):
        return len(self.outputs) == 0

    def exists (self, engine, schema):
        metadata = sq.MetaData()
        metadata.reflect(bind=engine, schema=schema)
        return all([output.exists(metadata) for output in self.outputs])

    def run (self, engine, constring, schema):
        for cmd in self.commands:
            cmd.run(engine, constring, schema)

    @staticmethod
    def from_yaml(yaml):
        commands = yaml['commands'] if 'commands' in yaml else []
        outputs = yaml['outputs'] if 'outputs' in yaml else []
        deps = yaml['depends_on'] if 'depends_on' in yaml else []
        if isinstance(deps, str):
            deps = [deps] # force singleton to list
        return Task(yaml['name'], [Command.from_yaml(c) for c in commands], [Output.from_yaml(o) for o in outputs], deps)

class TaskFailedError(Exception):
    pass
