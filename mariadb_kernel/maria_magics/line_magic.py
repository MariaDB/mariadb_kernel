"""The type representing a line magic"""

"""
A line magic is a shortcut a user can write into a notebook cell.
The line magic is written under the form '%magic' and in principle
it only sees what is passed as arguments, e.g. '%magic arg1 arg2'
"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.maria_magic import MariaMagic

import base64
from distutils import util
from matplotlib import pyplot
import os
import shlex

class LineMagic(MariaMagic):
    def type(self):
        return "Line"

    """
    Casts a string to int, float, bool or return the input string
    """
    def _str_to_obj(self, s):
        try:
            return int(s)
        except ValueError:
            try:
                return float(s)
            except ValueError:
                pass

        try:
            return bool(util.strtobool(s))
        except ValueError:
            return s


    """
    Gets a string of "key=value" space-separated arguments
    (e.g. input="kind="hist" bins=8 alpha=0.3")
    and parses it into a Python dict.
    """
    def parse_args(self, input):
        d = dict(token.split('=') for token in shlex.split(input))
        for k,v in d.items():
            d[k] = self._str_to_obj(v)

        return d

    def generate_plot(self, kernel, data, plot_type):
        image_name = 'last_select.png'
        df = data['last_select']
                # Override the plot kind in case the user passes this option

        # When opening an existing notebook, the user can execute a cell
        # containing a magic command, but kernel has no SELECT result stored
        # because there is no query executed in this session
        if df.empty:
            err = 'There is no query previously executed. No data to plot'
            kernel._send_message('stderr', err)
            return

        try:
            d = self.parse_args(self.args)
        except ValueError:
            kernel._send_message(
                'stderr',
                "There was an error while parsing the arguments. "
                "Please check %lsmagic on how to use the magic command")
            return

        # Override the plot kind in case the user passes this option
        d['kind'] = plot_type

        # Because the DataFrame.plot.pie function only takes the 'y' axis as
        # option, let's offer users the possibility to specify the index of
        # the dataframe (which .pie() will use by default as the 'x' axis)
        # like they could customize it using DataFrame.set_index() from Python
        if plot_type == 'pie' and 'index' in d:
            try:
                df = df.set_index(d['index'])
            except KeyError:
                kernel._send_message('stderr', 'Index does not exist')
                return
            d.pop('index', None)

        try:
            df.plot(**d)
        except (ValueError, AttributeError, TypeError) as e:
            kernel._send_message('stderr', str(e))
            return

        pyplot.savefig(image_name)

        with open(image_name, 'rb') as f:
            img = f.read()
            image = base64.b64encode(img).decode('ascii')

            display_content = {"data": {"image/png": image}, "metadata": {}}
            kernel.send_response(kernel.iopub_socket, "display_data", display_content)

        os.unlink(image_name)
