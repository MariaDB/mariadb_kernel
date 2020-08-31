"""A wrapper for managing a testing-purposes instance of MariaDB server

Given an existing installation of MariaDB Server, the kernel tries to detect
if there is a server already running locally.
If there isn't one, to simplify users first time experience, the kernel
tries to spin a new instance for testing purposes and issues a warning.
"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

import subprocess
import signal
import time

class MariaDBServer:
    def __init__(self, log):
        self.log = log

    def start(self):
        self.server = subprocess.Popen(
                ["mariadbd"], stdout = subprocess.PIPE,
                stderr = subprocess.PIPE)

        self._wait_server(self.server.stderr, b"mariadbd: ready for connections")
        self.log.info("Started MariaDB server successfully")

    def stop(self):
        self.server.send_signal(signal.SIGQUIT)
        self._wait_server(self.server.stderr, b"mariadbd: Shutdown complete")
        self.log.info("Stopped MariaDB server successfully")

    def _wait_server(self, stream, msg):
        while True:
            line = stream.readline()
            if msg in line:
                return
