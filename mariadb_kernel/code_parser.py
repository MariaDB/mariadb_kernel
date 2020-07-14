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


class CodeParser:
    def __init__(self, cell_code):
        self.code = cell_code
        self.magics = []
        self.sql = ""
        self._parse()

    def get_sql(self):
        return self.sql

    def get_magics(self):
        return self.magics

    def _is_magic(self, code):
        return code.startswith("%")

    def _parse_magic(self, code):
        # TODO: Create actual magic objects
        if code.startswith("%%"):
            magic = code.split(" ", 1)[0][2:]
            return f"test cellmagic: {magic}"

        magic = code.split(" ", 1)[0][1:]
        return f"test linemagic: {magic}"

    def _parse(self):
        lines = self.code.split("\n")
        i = 0
        while i < len(lines):
            line = lines[i]
            if self._is_magic(line):
                self.magics.append(self._parse_magic(line))
                i += 1
                continue
            j = i
            code = ""
            while j < len(lines):
                code = code + lines[j] + " "
                if lines[j].find(";") >= 0:
                    break
                j += 1

            # Raise an exception if no SQL delimiter was found
            if j == len(lines):
                raise ValueError("No delimiter was found in the SQL code")

            self.sql += code
            i = j + 1
