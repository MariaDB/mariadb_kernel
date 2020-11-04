"""Class that creates the magic objects"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.maria_magic import MariaMagic
from mariadb_kernel.maria_magics import supported_magics

class MagicFactory:
    def __init__(self, log):
        self.log = log

    def create_magic(self, magic_cmd, args):
        magics = supported_magics.get()
        if not magic_cmd in magics:
            return ErrorMagic(magic_cmd)

        magic_type = magics[magic_cmd]
        return magic_type(args)

class ErrorMagic(MariaMagic):
    def __init__(self, name):
        self.name = name
    def execute(self, kernel, data):
        msg = f'The %{self.name} magic command does not exist'
        kernel._send_message('stderr', msg)

