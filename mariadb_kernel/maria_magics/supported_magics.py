""" Maintains a list of magic commands supported by the kernel """

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.line import Line
from mariadb_kernel.maria_magics.df import DF
from mariadb_kernel.maria_magics.lsmagic import LSMagic
from mariadb_kernel.maria_magics.maria_magic import MariaMagic
from mariadb_kernel.maria_magics.bar import Bar
from mariadb_kernel.maria_magics.pie import Pie

def get():
    return {
        "line": Line,
        "bar": Bar,
        "pie": Pie,
        "df": DF,
        "lsmagic": LSMagic
    }

