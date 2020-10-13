"""Class that creates the magic objects"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.maria_magic import MariaMagic
import mariadb_kernel.maria_magics.supported_magics

class MagicFactory:
    def __init__(self, log):
        self.log = log

    def create_magic(self, magic_cmd, args):
        magics = supported_magics.get()
        if not magic_cmd in magics:
            self.log.info(f"The %{magic_cmd} magic command does not exist")
            return ErrorMagic()

        magic_type = magics[magic_cmd]
        return magic_type(args)

class ErrorMagic(MariaMagic):
    def execute(self, kernel, data):
        #TODO: it needs to alter result_dict so that it shows an error
        pass

