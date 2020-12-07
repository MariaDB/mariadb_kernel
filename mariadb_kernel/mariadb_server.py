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
    def __init__(self, log, config):
        self.log = log
        self.config = config

        server_bin = config.server_bin()
        # server_bin can either be an absolute path or just the name of the bin
        self.server_name = server_bin.split('/')[-1]
        self.server_up = False

    def start(self):
        server_bin = self.config.server_bin()
        try:
            self.server = subprocess.Popen(
                    [server_bin], stdout = subprocess.PIPE,
                    stderr = subprocess.PIPE,
                    universal_newlines=True)
        except FileNotFoundError:
            self.log.error(f"No MariaDB Server found at {server_bin};")
            self.log.error("Please install MariaDB from mariadb.org/download")
            self.log.error("If this is on purpose, you are limited to "
                           "connecting to remote databases only")
            return



        msg = f"{self.server_name}: ready for connections"
        self._wait_server(self.server.stderr, msg)
        self.log.info("Started MariaDB server successfully")
        self.server_up = True

    def stop(self):
        if not self.is_up():
            return

        self.server.send_signal(signal.SIGQUIT)
        msg = f"{self.server_name}: Shutdown complete"
        self._wait_server(self.server.stderr, msg)
        self.log.info("Stopped MariaDB server successfully")
        self.server_up = False

    def _wait_server(self, stream, msg):
        while True:
            line = stream.readline()
            if msg in line:
                return

    def is_up(self):
        return self.server_up
