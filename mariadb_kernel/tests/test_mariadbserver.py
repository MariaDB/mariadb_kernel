import pytest
from subprocess import check_output
from unittest.mock import Mock

def test_mariadb_server_logs_error_when_serverbin_invalid(mariadb_server):
    mocklog = Mock()
    mockconfig = Mock()
    server_bin = 'invalid_mysqld'
    mockconfig.server_bin.return_value = server_bin

    mariadb_server(mocklog, mockconfig)

    mocklog.error.assert_any_call(
        f"No MariaDB Server found at {server_bin};"
    )

def test_mariadb_server_starts_stops_mysqld_correctly(mariadb_server):
    mocklog = Mock()
    mockconfig = Mock()
    mockconfig.server_bin.return_value = 'mysqld'

    server = mariadb_server(mocklog, mockconfig)

    mocklog.info.assert_any_call("Started MariaDB server successfully")

    assert server.is_up() == True

    # Throws CalledProcessError when return value of pidof is non-zero
    check_output(["pidof", 'mysqld'])

    # It's fine to call this here, mariadb_server fixture won't do any harm
    # when it calls server.stop() too
    server.stop()
    mocklog.info.assert_any_call("Stopped MariaDB server successfully")

    assert server.is_up() == False

