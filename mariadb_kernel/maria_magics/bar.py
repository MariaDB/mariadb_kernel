"""This class implements the %bar magic command"""

help_text = """
The %bar magic command follows a syntax very similar to that of
DataFrame.plot.bar from Pandas.
Please refer to this link for an exhaustive list of options:
pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.plot.bar.html

Example:
    > %bar x=column1 y=column2 stacked=True

The whole purpose of this magic command is to allow the user to display
the result of the last query (e.g. SELECT, SHOW,...) in a nice and simple
matplotlib plot.

Internally, the Bar class receives the data of the last query from the kernel
as a Pandas DataFrame, it generates a plot PNG image, wraps the image into
a nice display_data Jupyter message and then sends it further.
"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.
from mariadb_kernel.maria_magics.line_magic import LineMagic



class Bar(LineMagic):
    def __init__(self, args):
        self.args = args

    def name(self):
        return '%bar'

    def help(self):
        return help_text

    def execute(self, kernel, data):
        self.generate_plot(kernel, data, 'bar')

