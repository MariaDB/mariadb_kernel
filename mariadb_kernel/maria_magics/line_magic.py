"""The type representing a line magic"""

"""
A line magic is a shortcut a user can write into a notebook cell.
The line magic is written under the form '%magic' and in principle
it only sees what is passed as arguments, e.g. '%magic arg1 arg2'
"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.maria_magic import MariaMagic

class LineMagic(MariaMagic):
    def type(self):
        return "Line"
