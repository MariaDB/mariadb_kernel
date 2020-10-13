"""This class implements the %line magic command"""

help_text = """
The %line magic command has the following syntax:
    > %line [column1, column2,...]

The whole purpose of this magic command is to allow the user to display
the result of the last query (e.g. SELECT, SHOW,...) in a nice and simple
matplotlib plot.

If no arguments are specified, a plot of all the columns in the result set
is displayed. But sometimes this is not necessary, so users have the ability
to specify the columns of the result that the kernel should use for plotting.

Internally, the Line class receives the data of the last query from the kernel
as a Pandas DataFrame, it generates a plot PNG image, wraps the image into
a nice display_data Jupyter message and then sends it further.
"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.
from mariadb_kernel.maria_magics.line_magic import LineMagic


import base64
import os
from matplotlib import pyplot

class Line(LineMagic):
    def __init__(self, args):
        self.args = args
        self.columns = args.split(' ')

    def name(self):
        return '%line'

    def help(self):
        return help_text

    def execute(self, kernel, data):
        image_name = 'last_select.png'
        df = data['last_select']

        # When opening an existing notebook, the user can execute a cell
        # containing a %line magic, but kernel has no SELECT result stored
        # because there is no query executed in this session
        if df.empty:
            err = 'There is no query previously executed. No data to plot'
            kernel._send_message('stderr', err)
            return

        try:
            # Allow the magic command to be executed with no arguments.
            # Just plot the entire dataframe if no arguments are passed
            if self.args:
                df = df[self.columns]
        except KeyError as e:
            kernel._send_message('stderr', str(e))
            return

        if len(df.columns) > 1:
            df = df.set_index(df.columns[0])

        df.plot()
        pyplot.savefig(image_name)

        with open(image_name, 'rb') as f:
            img = f.read()
            image = base64.b64encode(img).decode('ascii')

            display_content = {"data": {"image/png": image}, "metadata": {}}
            kernel.send_response(kernel.iopub_socket, "display_data", display_content)

        os.unlink(image_name)
