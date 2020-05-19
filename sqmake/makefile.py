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

"""
Represents an SQMake Makefile
"""

import networkx as nx
import yaml
import os.path
import sqlalchemy as sq
from logging import getLogger

from .circular_dependency_error import CircularDependencyError
from .task import Task, TaskFailedError
from .util import withdir

LOG = getLogger(__name__)

class Makefile(object):
    def __init__ (self):
        self.tasks = {}

    def _resolve_dependencies (self):
        "resolve inter-task dependencies"
        self._dependencies = nx.DiGraph()

        for task_name, task in self.tasks.items():
            for ancestor in task.depends_on:
                if not ancestor in self.tasks:
                    raise KeyError(f'ancestor task {ancestor} of {task_name} does not exist!')
                self._dependencies.add_edge(ancestor, task_name)

        if not nx.algorithms.dag.is_directed_acyclic_graph(self._dependencies):
            # find all cycles
            cycles = ['>'.join(x) for x in nx.simple_cycles(self._dependencies)]
            raise CircularDependencyError('Dependency cycles present: ', ', '.join(cycles))

    def _add_task (self, task_name, task):
        if task_name in self.tasks:
            raise KeyError(f'task {task_name} already exists!')
        if '/' in task_name:
            raise KeyError(f'Task name {task_name} includes reserved character /')
        self.tasks[task_name] = task

    def _include (self, namespace, makefile):
        'Include another makefile in this one'
        for task_name, task in makefile.tasks.items():
            # resolve internal references
            new_deps = []
            for dep in task.depends_on:
                if dep in makefile.tasks:
                    new_deps.append(f'{namespace}/{dep}')
                else:
                    # fully qualified
                    new_deps.append(dep)

            task.depends_on = new_deps

            # don't call _add_task because it checks for slashes
            self.tasks[f'{namespace}/{task_name}'] = task

    def run (self, task_name, engine=None):
        if engine is None:
            engine = sq.create_engine(self.db)

        task = self.tasks[task_name]
        # check if task needs to be run
        if not task.exists(engine, self.schema):
            LOG.info(f'running task {task_name}')
            LOG.info('checking dependencies')
            for dependency in task.depends_on:
                self.run(dependency, engine=engine)

            task.run(engine)

            if not task.exists(engine, self.schema):
                raise TaskFailedError(f'task {task_name} did not produce required outputs')
        else:
            LOG.info(f'{task_name} already complete, skipping')

    @staticmethod
    def from_yaml (filename):
        with open(filename) as inf:
            parsed = yaml.safe_load(inf)

        makefile = Makefile()
        makefile.db = parsed['db'] if 'db' in parsed else None
        makefile.schema = parsed['schema'] if 'schema' in parsed else None
        if 'tasks' in parsed:
            for task in parsed['tasks']:
                makefile._add_task(task['name'], Task.from_yaml(task))
        
        dirname = os.path.dirname(filename)
        if 'includes' in parsed:
            for include in parsed['includes']:
                subfilename = os.path.join(dirname, include['file'])
                subdirname = os.path.dirname(subfilename)
                subfileonly = os.path.basename(subfilename)
                with withdir(subdirname):
                    submakefile = makefile.from_yaml(subfileonly)
                makefile._include(include['name'], submakefile)
        
        makefile._resolve_dependencies()
        return makefile