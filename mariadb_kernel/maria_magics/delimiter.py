"""This class implements the %%delimiter magic command"""

help_text = """
The %%delimiter magic command is a cell magic. This means
that it operates over the entire cell within it is used.

Its purpose is to run an SQL statement using a different
delimiter than the default ";". The main usecase should be
Stored Procedures and Stored Functions.

Example:
--------cell
%%delimiter //
CREATE PROCEDURE proc ()
 BEGIN
  select 1;
 END;
//
--------end-of-cell

Please note that the SQL statement needs to end with the
delimiter specified by the magic command.
"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.
from mariadb_kernel.maria_magics.cell_magic import CellMagic


class Delimiter(CellMagic):
    def __init__(self, args):
        self.args = args

    def name(self):
        return "%%delimiter"

    def help(self):
        return help_text

    def execute(self, kernel, data):
        delimiter = self.args["args"]
        code = self.args["code"]
        delimiter_bkp = kernel.get_delimiter()
        kernel.set_delimiter(delimiter)
        try:
            kernel.do_execute(code, silent=False)
        finally:
            kernel.set_delimiter(delimiter_bkp)
