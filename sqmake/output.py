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

import os.path

class Output(object):
    def exists (self, con):
        pass

    @staticmethod
    def from_yaml (o):
        table = False
        column = False
        fle = False
        if 'table' in o:
            table = True
        if 'column' in o:
            column = True
        if 'file' in o:
            fle = True

        if fle and column or fle and table:
            raise ValueError('file output cannot be combined with table/column output')

        if column and not table:
            raise ValueError('Column output must have table property!')

        if column and table:
            return ColumnOutput(o['table'], o['column'])

        if table:
            return TableOutput(o['table'])

        if fle:
            return FileOutput(o['file'])

class TableOutput(Output):
    def __init__ (self, table):
        self.table = table

    def exists (self, metadata):
        return self.table in metadata.tables

class ColumnOutput(Output):
    def __init__ (self, table, column):
        self.table = table
        self.column = column

    def exists (self, metadata):
        return self.table in metadata.tables and self.column in metadata.tables[self.table].columns

class FileOutput(Output):
    def __init__ (self, filename):
        self.filename = filename

    def exists (self, metadata):
        return os.path.exists(self.filename)