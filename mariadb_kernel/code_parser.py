"""A simple parser for extracting some meaning out of a code cell

The parser walks to the code coming from the kernel and separates it into
SQL code and magic commands.
The SQL code is passed further by the kernel to the MariaDB client for
execution.
The magic objects created here are invoked in the kernel to perform
their duties.
"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.magic_factory import MagicFactory


class CodeParser:
    def __init__(self, log, cell_code, delimiter):
        self.code = cell_code
        self.magics = []
        self.sql = []
        self.log = log
        self.delimiter = delimiter
        self.magic_factory = MagicFactory(log)
        self._parse()

    def get_sql(self):
        return self.sql

    def get_magics(self):
        return self.magics

    def _is_magic(self, code):
        return code.startswith("%")

    def _is_line_magic(self, code):
        return code.startswith("%") and not code.startswith("%%")

    def _is_cell_magic(self, code):
        return code.startswith("%%")

    def _parse(self):
        split = self.code.split("\n", maxsplit=1)
        first_line = split[0].lstrip()
        if self._is_magic(first_line):
            magic = first_line.split(" ", maxsplit=1)
            magic_code = ""
            args = ""
            if len(magic) > 1:
                args = magic[1]

            if self._is_line_magic(first_line):
                magic_cmd = magic[0][1:]
                magic_obj = self.magic_factory.create_magic(magic_cmd, args)
                self.magics.append(magic_obj)
            elif self._is_cell_magic(first_line):
                magic_cmd = magic[0][2:]
                if len(split) > 1:
                    magic_code = split[1].strip()
                magic_obj = self.magic_factory.create_magic(
                    magic_cmd, {"args": args, "code": magic_code}
                )
                self.magics.append(magic_obj)
            return

        code = self.code.strip()
        if not code.endswith(self.delimiter):
            raise ValueError(
                f"Your SQL code doesn't end with delimiter `{self.delimiter}`"
            )

        self.sql.append(code)
