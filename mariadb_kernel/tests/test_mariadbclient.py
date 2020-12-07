import pytest
from subprocess import check_output
from unittest.mock import Mock

from ..mariadb_client import MariaDBClient, ServerIsDownError, LoginError
from ..client_config import ClientConfig

def test_mariadb_client_logs_error_when_clienbin_invalid():
    mocklog = Mock()
    mockconfig = Mock()
    client_bin = 'invalid_mysql'
    mockconfig.client_bin.return_value = client_bin

    client = MariaDBClient(mocklog, mockconfig)
    client.start()

    mocklog.error.assert_any_call(
        f"No mariadb> command line client found at {client_bin};"
    )

def test_mariadb_client_raises_when_server_is_down():
    mocklog = Mock()
    mockconfig = Mock()

    # Give the client a wrong port, simulate that MariaDB Server is down
    mockconfig.client_bin.return_value = "mysql"
    mockconfig.get_args.return_value = "--port=0000"

    client = MariaDBClient(mocklog, mockconfig)

    with pytest.raises(ServerIsDownError):
        client.start()

    mocklog.error.assert_any_call("MariaDB client failed to start")
    mocklog.error.assert_any_call("Most probably the MariaDB server is not started")

def test_mariadb_client_raises_when_credentials_are_wrong(mariadb_server):
    mocklog = Mock()
    mockconfig = Mock()

    # Start the server
    mockconfig.server_bin.return_value = 'mysqld'
    mariadb_server(mocklog, mockconfig)

    # Give the client a wrong port, simulate that MariaDB Server is down
    mockconfig.client_bin.return_value = "mysql"
    mockconfig.get_args.return_value = '--user=root --password="somewrongpass" --port=3306'

    client = MariaDBClient(mocklog, mockconfig)

    with pytest.raises(LoginError):
        client.start()

    mocklog.error.assert_any_call("MariaDB client failed to start")
    mocklog.error.assert_any_call("The credentials used for connecting are wrong")

def test_mariadb_client_run_statement(mariadb_server):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name='nonexistentcfg.json')
    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)

    client.start()

    result = client.run_statement("select 1;")

    assert result == "<TABLE BORDER=1><TR><TH>1</TH></TR><TR><TD>1</TD></TR></TABLE>"

    result = client.run_statement("select a from not_a_table;")

    assert result.startswith("ERROR")

    assert client.iserror()

    assert result == client.error_message()

