""" Maintains a list of magic commands supported by the kernel """

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.line import Line
from mariadb_kernel.maria_magics.df import DF
from mariadb_kernel.maria_magics.lsmagic import LSMagic
from mariadb_kernel.maria_magics.maria_magic import MariaMagic

def get():
    return {
        "line": Line,
        "df": DF,
        "lsmagic": LSMagic
    }

