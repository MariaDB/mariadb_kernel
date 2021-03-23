import pytest
from subprocess import check_output
from unittest.mock import Mock

from ..mariadb_client import MariaDBClient, ServerIsDownError, LoginError
from ..client_config import ClientConfig


def test_mariadb_client_logs_error_when_clienbin_invalid():
    mocklog = Mock()
    mockconfig = Mock()
    client_bin = "invalid_mysql"
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
    mockconfig.server_bin.return_value = "mysqld"
    mariadb_server(mocklog, mockconfig)

    # Give the client a wrong port, simulate that MariaDB Server is down
    mockconfig.client_bin.return_value = "mysql"
    mockconfig.get_args.return_value = (
        '--user=root --password="somewrongpass" --port=3306'
    )

    client = MariaDBClient(mocklog, mockconfig)

    with pytest.raises(LoginError):
        client.start()

    mocklog.error.assert_any_call("MariaDB client failed to start")
    mocklog.error.assert_any_call("The credentials used for connecting are wrong")


def test_mariadb_client_run_statement(mariadb_server):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")
    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)

    client.start()

    result = client.run_statement("select 1;")

    assert result == "<TABLE BORDER=1><TR><TH>1</TH></TR><TR><TD>1</TD></TR></TABLE>"

    result = client.run_statement("select a from not_a_table;")

    assert result.startswith("ERROR")

    assert client.iserror()

    assert result == client.error_message()

    # Client should say a "Query OK" for queries that don't return a result
    client.run_statement("create database if not exists test;")
    result = client.run_statement("use test;")
    assert result == "Query OK"

def test_multi_line_output(mariadb_server):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")
    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)

    client.start()

    result = client.run_statement(
        """select json_detailed('["an array", "of", "json", {"objects": ["embedded", 1, 2, 3]}]');"""
    )

    # Debug info in case something changes in the output and test starts failing
    print(repr(result))

    # The expected output should contain a cell with the prettyfied JSON output
    expected = """<TABLE BORDER=1><TR><TH>json_detailed('[&quot;an array&quot;, &quot;of&quot;, &quot;json&quot;, {&quot;objects&quot;: [&quot;embedded&quot;, 1, 2, 3]}]')</TH></TR><TR><TD>[\r\n    &quot;an array&quot;,\r\n    &quot;of&quot;,\r\n    &quot;json&quot;,\r\n    \r\n    {\r\n        &quot;objects&quot;: \r\n        [\r\n            &quot;embedded&quot;,\r\n            1,\r\n            2,\r\n            3\r\n        ]\r\n    }\r\n]</TD></TR></TABLE>"""

    assert result == expected
