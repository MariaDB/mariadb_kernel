"""Class that creates the magic objects"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.line import Line
from mariadb_kernel.maria_magics.df import DF
import mariadb_kernel.maria_magics.lsmagic as lsmagic
from mariadb_kernel.maria_magics.maria_magic import MariaMagic

class MagicFactory:
    supported_magics = {
        "line": Line,
        "df": DF,
        "lsmagic": lsmagic.LSMagic
    }

    @staticmethod
    def magics():
        return MagicFactory.supported_magics

    def __init__(self, log):
        self.log = log

    def create_magic(self, magic_cmd, args):
        if not magic_cmd in MagicFactory.supported_magics:
            self.log.info(f"The %{magic_cmd} magic command does not exist")
            return ErrorMagic()

        magic_type = MagicFactory.supported_magics[magic_cmd]
        return magic_type(args)

class ErrorMagic(MariaMagic):
    def execute(self, kernel, data):
        #TODO: it needs to alter result_dict so that it shows an error
        pass

