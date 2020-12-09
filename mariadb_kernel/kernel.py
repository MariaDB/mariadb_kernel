"""The MariaDB kernel"""
# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from ipykernel.kernelbase import Kernel

from ._version import version as __version__
from mariadb_kernel.client_config import ClientConfig
from mariadb_kernel.mariadb_client import MariaDBClient, ServerIsDownError
from mariadb_kernel.code_parser import CodeParser
from mariadb_kernel.mariadb_server import MariaDBServer
from mariadb_kernel.maria_magics.maria_magic import MariaMagic

import logging
import pexpect
import re
import pandas


class MariaDBKernel(Kernel):
    implementation = "MariaDB"
    implementation_version = __version__
    language_info = {
        "name": "SQL",
        "mimetype": "text/plain",
        "file_extension": ".sql",
    }
    banner = "MariaDB kernel"

    def __init__(self, **kwargs):
        Kernel.__init__(self, **kwargs)
        self.log.setLevel(logging.INFO)
        self.client_config = ClientConfig(self.log)
        self.mariadb_client = MariaDBClient(self.log, self.client_config)
        self.mariadb_server = None
        self.data = {
            "last_select": pandas.DataFrame([])
        }

        try:
            self.mariadb_client.start()
        except ServerIsDownError:
            if not self.client_config.start_server():
                self.log.error("The options passed through mariadb_kernel.json "
                               "prevent the kernel from starting a testing "
                               "MariaDB server instance")
                raise

            # Start a single MariaDB server for a better experience
            # if user wants to quickly test the kernel
            self.mariadb_server = MariaDBServer(self.log, self.client_config)
            self.mariadb_server.start()

            # Reconnect the client now that the server is up
            if self.mariadb_server.is_up():
                self.mariadb_client.start()


    def _execute_magics(self, magics):
        for magic in magics:
            magic.execute(self, self.data)

    def _update_data(self, result_html):
        if not result_html or not result_html.startswith('<TABLE'):
            return

        df = pandas.read_html(result_html)
        self.data["last_select"] = df[0]

    def _send_message(self, stream, message):
        error = {'name': stream, 'text': message + '\n'}
        self.send_response(self.iopub_socket, 'stream', error)


    def do_execute(
        self, code, silent, store_history=True, user_expressions=None, allow_stdin=False
    ):
        rv = {
            "status": "ok",
            # The base class increments the execution count
            "execution_count": self.execution_count,
            "payload": [],
            "user_expressions": {},
        }

        try:
            parser = CodeParser(self.log, code)
        except ValueError as e:
            self._send_message('stderr', str(e))
            return rv

        statements = parser.get_sql()
        for s in statements:
            result = self.mariadb_client.run_statement(s)
            if self.mariadb_client.iserror():
                self._send_message('stderr', self.mariadb_client.error_message())
                continue

            self._update_data(result)
            display_content = {"data": {"text/html": str(result)}, "metadata": {}}
            if not silent:
                self.send_response(self.iopub_socket, "display_data", display_content)

        self._execute_magics(parser.get_magics())

        return rv

    def do_shutdown(self, restart):
        self.mariadb_client.stop()
        if self.mariadb_server:
                self.mariadb_server.stop()

    def do_complete(self, code, cursor_pos):
        return {"status": "ok", "matches": ["test"]}
