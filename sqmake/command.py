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
import tqdm
import pandas as pd
import subprocess
import sqlalchemy as sq
import os
import platform
import shutil

LOG = getLogger(__name__)

class Command(object):
    def __init__ (self, fn, code):
        'handle abstraction of file or direct code'
        if code is None and fn is not None:
            with open(fn) as inf:
                self.code = inf.read()
        else:
            self.code = code

    def run (self, engine, constring, schema):
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
        elif yaml['type'] == 'data':
            init_code = yaml['init_code'] if 'init_code' in yaml else None
            table = yaml['table'] if 'table' in yaml else None
            cleanup_code = yaml['cleanup_code'] if 'cleanup_code' in yaml else None
            return DataCommand(fn, init_code, cleanup_code, table)
        else:
            raise ValueError(f'Unknown type {yaml["type"]}')

class SqlCommand(Command):
    def __init__ (self, fn, code):
        super().__init__(fn, code)

    def run (self, engine, constring, schema):
        LOG.info(f'sql> {self.code}')
        with engine.begin() as con:
            con.execute(sq.text(self.code))

class ShellCommand(Command):
    def __init__ (self, fn, code):
        super().__init__(fn, code)

    def run (self, engine, constring, schema):
        LOG.info(f'sh> {self.code}')
        # TODO how to handle working directory for this process?
        shell = None
        if platform.system() == "Windows":
            # always run commands in a Unix shell, find git bash on Windows
            shell = shutil.which("bash")
        
        process = subprocess.Popen(self.code, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env={**os.environ, 'SQMAKE_DB': constring, 'SQMAKE_SCHEMA': schema}, executable=shell)

        while True:
            retcode = process.poll()
            if retcode is not None:
                break
            try:
                stdout, stderr = process.communicate(timeout=1)
            except subprocess.TimeoutExpired:
                pass # poll again
            else:
                LOG.info(stdout)
                LOG.info(stderr)

        if retcode != 0:
            raise CommandFailedError('Command terminated with non-zero return code {}'.format(retcode))

class DataCommand(Command):
    def __init__ (self, fn, init_code, cleanup_code, table):
        super().__init__(None, None)
        self.fn = fn
        self.init_code = init_code
        self.cleanup_code = cleanup_code
        self.table = table

    def run (self, engine, constring, schema):
        with engine.begin() as conn:
            meta = sq.MetaData(conn)

            if self.init_code is not None:
                LOG.info(f'sql> {self.init_code}')
                conn.execute(self.init_code)

            if '.' in self.table:  # TODO this fails with complex table names that contain periods. Let's assume no one does that.
                schema, table = self.table.split('.')
            else:
                table = self.table
                schema = None

            total_rows = None # unknown total rows
            if self.fn.lower().endswith('.csv'):
                with open(self.fn) as s:
                    total_rows = 0
                    for line in s:
                        total_rows += 1
                    total_rows -= 1 # header
                chunk_source = pd.read_csv(self.fn, chunksize=1000)
            elif self.fn.lower().endswith('.xlsx'):
                # excel files kinda have to fit in memory...
                chunk_source = [pd.read_excel(self.fn)]
                total_rows = len(chunk_source[0])
            else:
                raise CommandFailedError(f'Unrecognized format for file {self.fn}')

            with tqdm.tqdm(total=total_rows) as pbar:
                for chunk in chunk_source:
                    chunk.columns = [c.lower() for c in chunk.columns]
                    vals = list([t._asdict() for t in chunk.itertuples(index=False)])
                    conn.execute(sq.insert(sq.Table(table, meta, autoload=True, schema=schema)), vals)
                    pbar.update(len(chunk))

            if self.cleanup_code is not None:
                LOG.info(f'sql> {self.cleanup_code}')
                conn.execute(self.cleanup_code)


class CommandFailedError(Exception):
    pass
