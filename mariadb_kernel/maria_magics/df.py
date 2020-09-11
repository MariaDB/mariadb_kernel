"""This class implements the %df magic command"""

help_text = """
The %df magic command has the following syntax:
    > %df [filename]

It writes the result of the last query executed in the notebook
into an external CSV formatted file.
The purpose of this magic command is to allow users to export query
data from their MariaDB databases and then quickly import it
into a Python Notebook where more complex analytics can be performed.

If no arguments are specified, the kernel writes the data into a
CSV file named 'last_query.csv'.
"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.
from mariadb_kernel.maria_magics.line_magic import LineMagic


import os

class DF(LineMagic):
    def __init__(self, filename):
        self.filename = 'last_query.csv'
        if filename:
            self.filename = filename

    def name(self):
        return '%df'

    def help(self):
        return help_text

    def execute(self, kernel, data):
        df = data['last_select']

        # When opening an existing notebook, the user can execute a cell
        # containing a %df magic, but kernel has no SELECT result stored
        # because there is no query executed in this session
        if df.empty:
            err = 'There is no query previously executed. No data to write'
            kernel._send_message('stderr', err)
            return

        df.to_csv(self.filename, index=False)

        message = f'The result set was successfully written into {self.filename}'
        kernel._send_message('stdout', message)

