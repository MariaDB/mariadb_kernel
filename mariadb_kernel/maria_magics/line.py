"""TODO"""

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
        return 'line'

    def execute(self, kernel, data):
        image_name = 'last_select.png'
        df = data['last_select']

        # When opening an existing notebook, the user can execute a cell
        # containing a %line magic, but kernel has no SELECT result stored
        # because there is no query executed in this session
        if df.empty:
            err = 'There is no query previously executed. No data to plot'
            kernel._send_error(err)
            return

        try:
            # Allow the magic command to be executed with no arguments.
            # Just plot the entire dataframe if no arguments are passed
            if self.args:
                df = df[self.columns]
        except KeyError as e:
            kernel._send_error(str(e))
            return

        df.plot()
        pyplot.savefig(image_name)

        with open(image_name, 'rb') as f:
            img = f.read()
            image = base64.b64encode(img).decode('ascii')

            display_content = {"data": {"image/png": image}, "metadata": {}}
            kernel.send_response(kernel.iopub_socket, "display_data", display_content)

        os.unlink(image_name)
