"""The type representing a cell magic"""

"""
A cell magic is a shortcut a user can write into a notebook cell.
The cell magic is written under the form '%%magic' and in principle
it operates over the entire code of the cell it is written in, e.g.

------cell
%%magic_python
from matplotlib import pyplot
x = [1,2,3]
print(x)
pyplot.plot(x)
------end of cell

"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.maria_magic import MariaMagic

class CellMagic(MariaMagic):
    def type(self):
        return "Cell"
