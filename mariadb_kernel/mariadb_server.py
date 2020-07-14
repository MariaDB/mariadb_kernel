"""A wrapper for managing a testing-purposes instance of MariaDB server

Given an existing installation of MariaDB Server, the kernel tries to detect
if there is a server already running locally.
If there isn't one, to simplify users first time experience, the kernel
tries to spin a new instance for testing purposes and issues a warning.
"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.


class MariaDBServer:
    def __init__(self):
        pass

    def start(self):
        pass

    def stop(self):
        pass

    def server_ready(self):
        pass
