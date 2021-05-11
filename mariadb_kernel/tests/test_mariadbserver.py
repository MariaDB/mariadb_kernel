import pytest
from subprocess import check_output
from unittest.mock import Mock

from ..client_config import ClientConfig


def test_mariadb_server_logs_error_when_serverbin_invalid(mariadb_server):
    mocklog = Mock()
    server_bin = "invalid_mysqld"

    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    # Give the kernel a wrong mysqld binary
    cfg.default_config.update({"server_bin": server_bin})

    mariadb_server(mocklog, cfg)

    mocklog.error.assert_any_call(f"No MariaDB Server found at {server_bin};")


def test_mariadb_server_starts_stops_mysqld_correctly(mariadb_server):
    mocklog = Mock()

    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    server = mariadb_server(mocklog, cfg)

    mocklog.info.assert_any_call("Started MariaDB server successfully")

    assert server.is_up() == True

    # Throws CalledProcessError when return value of pidof is non-zero
    check_output(["pidof", "mysqld"])

    # It's fine to call this here, mariadb_server fixture won't do any harm
    # when it calls server.stop() too
    server.stop()
    mocklog.info.assert_any_call("Stopped MariaDB server successfully")

    # Throws TimeoutExpired if the server didn't die
    server.server.wait(timeout=3)

    assert server.is_up() == False
