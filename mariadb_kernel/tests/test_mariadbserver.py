import pytest
from subprocess import check_output
from unittest.mock import Mock

from ..mariadb_server import MariaDBServer

def test_mariadb_server_logs_error_when_serverbin_invalid():
    mocklog = Mock()
    mockconfig = Mock()
    server_bin = 'invalid_mysqld'
    mockconfig.server_bin.return_value = server_bin

    server = MariaDBServer(mocklog, mockconfig)
    server.start()

    mocklog.error.assert_any_call(
        f"No MariaDB Server found at {server_bin};"
    )

def test_mariadb_server_starts_mysqld_correctly():
    mocklog = Mock()
    mockconfig = Mock()
    mockconfig.server_bin.return_value = 'mysqld'

    server = MariaDBServer(mocklog, mockconfig)
    server.start()

    mocklog.info.assert_any_call("Started MariaDB server successfully")

    assert server.is_up() == True

    # Throws CalledProcessError when return value of pidof is non-zero
    check_output(["pidof", 'mysqld'])

    server.stop()
    mocklog.info.assert_any_call("Stopped MariaDB server successfully")

    assert server.is_up() == False

