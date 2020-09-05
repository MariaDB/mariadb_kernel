"""TODO"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.
from mariadb_kernel.maria_magics.line_magic import LineMagic

class Line(LineMagic):
    def __init__(self, args):
        self.args = args

    def name(self):
        return "line"

    def execute(self, result_dict, data):
        return False
