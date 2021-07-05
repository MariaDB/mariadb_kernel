from typing import List, Type

from prompt_toolkit.completion.base import Completion
from ..mariadb_client import MariaDBClient

from ..mariadb_server import MariaDBServer
from ..client_config import ClientConfig

from unittest.mock import Mock
from ..autocompleter import Autocompleter

import unittest


def get_text_list(completions: List[Completion]):
    return [completion.text for completion in completions]


def test_mariadb_autocompleter_keywords_suggestion(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("use test;")
    autocompleter = Autocompleter(client, mocklog)
    assert set(["select"]).issubset(
        get_text_list(autocompleter.get_suggestions("sel", 3))
    )


def test_mariadb_autocompleter_default_functions_suggestion(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("use test;")
    autocompleter = Autocompleter(client, mocklog)
    assert set(autocompleter.completer.functions).issubset(
        get_text_list(autocompleter.get_suggestions("select ", 7))
    )


def test_mariadb_autocompleter_database_suggestion_after_use(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("use test;")
    autocompleter = Autocompleter(client, mocklog)
    assert set(["information_schema", "mysql", "performance_schema", "test"]).issubset(
        get_text_list(autocompleter.get_suggestions("use ", 4))
    )


def test_mariadb_autocompleter_table_suggestion_under_database_by_dot(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("use test;")
    client.run_statement("create table t1 (a int, b int, c int);")
    autocompleter = Autocompleter(client, mocklog)

    assert set(["t1"]).issubset(
        get_text_list(autocompleter.get_suggestions("insert into test.", 17))
    )

    client.run_statement("drop table t1;")


def test_mariadb_autocompleter_column_suggestion_after_where(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("use test;")
    client.run_statement("create table t1 (a int, b int, c int);")
    autocompleter = Autocompleter(client, mocklog)

    assert set(["t1"]).issubset(
        get_text_list(autocompleter.get_suggestions("select * from t1 where ", 23))
    )

    client.run_statement("drop table t1;")


def test_mariadb_autocompleter_column_suggestion_insert_into_clause(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("use test;")
    client.run_statement("create table t1 (a int, b int, c int);")
    autocompleter = Autocompleter(client, mocklog)

    unittest.TestCase().assertListEqual(
        ["*", "a", "b", "c"],
        get_text_list(autocompleter.get_suggestions("insert into t1 (", 16)),
    )

    client.run_statement("drop table t1;")


def test_mariadb_autocompleter_column_suggestion_by_table_aliase(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("use test;")
    client.run_statement("create table t1 (a int, b int, c int);")
    autocompleter = Autocompleter(client, mocklog)

    unittest.TestCase().assertListEqual(
        ["*", "a", "b", "c"],
        get_text_list(autocompleter.get_suggestions("select t. from t1 as t", 9)),
    )

    client.run_statement("drop table t1;")


def test_mariadb_autocompleter_show_variant_suggestion(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("use test;")
    autocompleter = Autocompleter(client, mocklog)

    # not complete
    assert set(
        [
            "AUTHORS",
            "BINARY LOGS",
            "BINLOG EVENTS",
            "CHARACTER SET",
            "COLLATION",
            "COLUMNS",
            "CONTRIBUTORS",
        ]
    ).issubset(get_text_list(autocompleter.get_suggestions("SHOW ", 5)))


def test_mariadb_autocompleter_username_at_hostname_suggestion(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("use test;")
    client.run_statement("create user foo2@test IDENTIFIED BY 'password';")
    autocompleter = Autocompleter(client, mocklog)

    # not complete
    assert set(
        [
            "'root'@'127.0.0.1'",
            "'root'@'localhost'",
            "'foo2'@'test'",
        ]
    ).issubset(get_text_list(autocompleter.get_suggestions("ALTER USER ", 11)))

    client.run_statement("drop foo2@test;")
