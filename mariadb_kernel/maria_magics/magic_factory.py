"""Class that creates the magic objects"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.line import Line
from mariadb_kernel.maria_magics.maria_magic import MariaMagic

class MagicFactory:
    def __init__(self, log):
        self.log = log
        self.supported_magics = {
            "line": Line
        }

    def create_magic(self, magic_cmd, args):
        if not magic_cmd in self.supported_magics:
            self.log.info(f"The %{magic_cmd} magic command does not exist")
            return ErrorMagic()

        magic_type = self.supported_magics[magic_cmd]
        return magic_type(args)

class ErrorMagic(MariaMagic):
    def execute(self, result_dict, data):
        #TODO: it needs to alter result_dict so that it shows an error
        pass

