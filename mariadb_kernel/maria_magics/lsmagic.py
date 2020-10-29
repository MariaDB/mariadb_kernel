"""This class implements %lsmagic magic command"""

help_text = """
The %lsmagic magic command prints the magics currently
supported by the kernel. It also prints the help text for each command
"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.line_magic import LineMagic
from mariadb_kernel.maria_magics import supported_magics

import os
from json2html import *

class LSMagic(LineMagic):
    def __init__(self, args):
        pass

    def name(self):
        return '%lsmagic'

    def help(self):
        return help_text

    def execute(self, kernel, data):
        result = { 'Line': [], 'Cell':[] }
        magics = supported_magics.get()
        for name, magic_type in magics.items():
            m = magic_type('')
            entry = {
                'name': m.name(),
                'help': m.help()
            }
            result[m.type()].append(entry)

        html = json2html.convert(json=result)

        # TODO: this is a hack, we should find some other solution for altering
        # the table elements styling
        html = html.replace('\n', '<br />')
        html = html.replace('<td>', '<td style="text-align: left;">')

        display_content = {"data": {"text/html": html}, "metadata": {}}
        kernel.send_response(kernel.iopub_socket, "display_data", display_content)

