import os
import pytest
import shutil
from pathlib import Path
from subprocess import check_output
from ..mariadb_server import MariaDBServer


@pytest.fixture
def mariadb_server():
    server = None

    def _server(log, config):
        nonlocal server
        server = MariaDBServer(log, config)
        server.start()
        return server

    yield _server

    server.stop()


@pytest.fixture(params=["line", "bar", "pie", "df", "lsmagic", "load"])
def magic_cmd(request):
    return request.param
