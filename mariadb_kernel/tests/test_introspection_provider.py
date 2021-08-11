from typing import Type

from prompt_toolkit.document import Document
from ..mariadb_client import MariaDBClient

from ..mariadb_server import MariaDBServer
from ..client_config import ClientConfig
from ..introspection_provider import IntrospectionProvider
from ..autocompleter import Autocompleter

from unittest.mock import Mock
import pytest


def test_introspection_provider_introspect_keyword(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    provider = IntrospectionProvider()
    autocompleter = Autocompleter(client, mocklog)
    autocompleter.refresh()

    result = provider.get_instropection(
        Document("select col1 from b;", len("sele")), autocompleter.completer
    )

    assert {"type": "keyword", "word": "select"} == result


def test_introspection_provider_introspect_function(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    provider = IntrospectionProvider()
    autocompleter = Autocompleter(client, mocklog)
    autocompleter.refresh()

    result = provider.get_instropection(
        Document("select min(col1) from b;", len("select mi")), autocompleter.completer
    )

    assert {"type": "function", "word": "min"} == result


def test_introspection_provider_introspect_database(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("create database mydb;")
    provider = IntrospectionProvider()
    autocompleter = Autocompleter(client, mocklog)
    autocompleter.refresh()

    result = provider.get_instropection(
        Document("use mydb;", len("use my")), autocompleter.completer
    )

    assert {"type": "database", "word": "mydb"} == result
    client.run_statement("drop database mydb;")


def test_introspection_provider_introspect_table(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("create database mydb;")
    client.run_statement("use mydb;")
    client.run_statement("create table tbl1 (col1 int);")
    provider = IntrospectionProvider()
    autocompleter = Autocompleter(client, mocklog)
    autocompleter.refresh()

    result = provider.get_instropection(
        Document("select col1 from tbl1", len("select col1 from tbl")),
        autocompleter.completer,
    )

    assert {"type": "table", "word": "tbl1", "database": "mydb"} == result
    client.run_statement("drop database mydb;")


def test_introspection_provider_introspect_table_that_is_not_belong_current_use_db(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("create database db1;")
    client.run_statement("create database db2;")
    client.run_statement("use db2;")
    client.run_statement("create table tbl1 (col1 int);")
    client.run_statement("use db1;")
    provider = IntrospectionProvider()
    autocompleter = Autocompleter(client, mocklog)
    autocompleter.refresh()

    result = provider.get_instropection(
        Document("insert into db2.tbl1 ", len("insert into db2.t")),
        autocompleter.completer,
    )

    assert {"type": "table", "word": "tbl1", "database": "db2"} == result
    client.run_statement("drop database db1;")
    client.run_statement("drop database db2;")


def test_introspection_provider_introspect_column(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table tbl1 (col1 int);")

    provider = IntrospectionProvider()
    autocompleter = Autocompleter(client, mocklog)
    autocompleter.refresh()

    result = provider.get_instropection(
        Document("select col1 from tbl1", len("select col")), autocompleter.completer
    )

    assert {
        "type": "column",
        "word": "col1",
        "database": "db1",
        "table": "tbl1",
    } == result
    client.run_statement("drop database db1;")


def test_introspection_provider_introspect_column_with_no_table_info(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table tbl1 (col1 int);")

    provider = IntrospectionProvider()
    autocompleter = Autocompleter(client, mocklog)
    autocompleter.refresh()

    result = provider.get_instropection(
        Document("select col1 from ", len("select col")), autocompleter.completer
    )

    assert {
        "type": "column",
        "word": "col1",
        "database": "db1",
        "table": "tbl1",
    } == result
    client.run_statement("drop database db1;")


def test_introspection_provider_introspect_column_with_column_name_is_same_with_function(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table tbl1 (min int);")

    provider = IntrospectionProvider()
    autocompleter = Autocompleter(client, mocklog)
    autocompleter.refresh()

    result = provider.get_instropection(
        Document("select min from tbl1;", len("select mi")), autocompleter.completer
    )

    assert {
        "type": "column",
        "word": "min",
        "database": "db1",
        "table": "tbl1",
    } == result
    client.run_statement("drop database db1;")


def test_introspection_provider_introspect_column_with_column_name_is_same_with_keyword(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table tbl1 (type int);")

    provider = IntrospectionProvider()
    autocompleter = Autocompleter(client, mocklog)
    autocompleter.refresh()

    result = provider.get_instropection(
        Document("select type from tbl1;", len("select ty")), autocompleter.completer
    )

    assert {
        "type": "column",
        "word": "type",
        "database": "db1",
        "table": "tbl1",
    } == result
    client.run_statement("drop database db1;")


def test_introspection_provider_introspect_function_with_function_name_is_same_with_column(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table tbl1 (min int);")

    provider = IntrospectionProvider()
    autocompleter = Autocompleter(client, mocklog)
    autocompleter.refresh()

    result = provider.get_instropection(
        Document("select min(min) from tbl1;", len("select mi")),
        autocompleter.completer,
    )

    assert {
        "type": "function",
        "word": "min",
    } == result
    client.run_statement("drop database db1;")


def test_introspection_provider_introspect_column_with_table_name_is_same_with_function(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table min (col1 int);")

    provider = IntrospectionProvider()
    autocompleter = Autocompleter(client, mocklog)
    autocompleter.refresh()

    result = provider.get_instropection(
        Document("select * from min;", len("select * from m")), autocompleter.completer
    )

    assert {
        "type": "table",
        "word": "min",
        "database": "db1",
    } == result
    client.run_statement("drop database db1;")


def test_introspection_provider_introspect_column_with_table_name_is_same_with_keyword(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table type (col1 int);")

    provider = IntrospectionProvider()
    autocompleter = Autocompleter(client, mocklog)
    autocompleter.refresh()

    result = provider.get_instropection(
        Document("select * from type;", len("select * from ty")),
        autocompleter.completer,
    )

    assert {
        "type": "table",
        "word": "type",
        "database": "db1",
    } == result
    client.run_statement("drop database db1;")


def test_introspection_provider_introspect_column_with_column_name_is_same_with_table(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table col_tabl (col1 int);")
    client.run_statement("create table tabl2 (col_tabl int);")

    provider = IntrospectionProvider()
    autocompleter = Autocompleter(client, mocklog)
    autocompleter.refresh()

    result = provider.get_instropection(
        Document("select tabl2.col_tabl from tabl2;", len("select tabl2.col_")),
        autocompleter.completer,
    )

    assert {
        "type": "column",
        "word": "col_tabl",
        "table": "tabl2",
        "database": "db1",
    } == result
    client.run_statement("drop database db1;")


def test_introspection_provider_introspect_insert_into_after_VALUES_would_suggest_column_hint(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table t1 (a int, b int, c int);")

    provider = IntrospectionProvider()
    autocompleter = Autocompleter(client, mocklog)
    autocompleter.refresh()

    result = provider.get_instropection(
        Document(
            "insert into t1 (a, b, c) VALUES (",
            len("insert into t1 (a, b, c) VALUES ("),
        ),
        autocompleter.completer,
    )

    assert {
        "type": "column_hint",
        "hint": "a",
        "table_name": "t1",
        "value_index": 0,
    } == result
    client.run_statement("drop database db1;")


@pytest.mark.parametrize(
    "input",
    [
        "insert into t1 (a, b, c) VALUES (1,2,",
        "insert into t1 (a, b, c) VALUES (1 , 2 , 3",
    ],
)
def test_introspection_provider_introspect_insert_into_after_VALUES_would_suggest_column_hint_when_cursor_in_third_value(
    mariadb_server: Type[MariaDBServer], input: str
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table t1 (a int, b int, c int);")

    provider = IntrospectionProvider()
    autocompleter = Autocompleter(client, mocklog)
    autocompleter.refresh()

    result = provider.get_instropection(
        Document(input, len(input)),
        autocompleter.completer,
    )

    assert {
        "type": "column_hint",
        "hint": "c",
        "table_name": "t1",
        "value_index": 2,
    } == result
    client.run_statement("drop database db1;")


def test_introspection_provider_introspect_insert_into_after_VALUES_would_suggest_column_hint_when_cursor_out_of_column_index(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table t1 (a int);")

    provider = IntrospectionProvider()
    autocompleter = Autocompleter(client, mocklog)
    autocompleter.refresh()

    result = provider.get_instropection(
        Document(
            "insert into t1 (a) VALUES (1,2", len("insert into t1 (a) VALUES (1,2")
        ),
        autocompleter.completer,
    )

    assert {
        "type": "column_hint",
        "hint": "out of column",
    } == result
    client.run_statement("drop database db1;")


def test_introspection_provider_introspect_insert_into_after_VALUES_would_suggest_column_hint_when_cursor_out_of_column_index_and_without_column_list(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table t1 (a int);")

    provider = IntrospectionProvider()
    autocompleter = Autocompleter(client, mocklog)
    autocompleter.refresh()

    result = provider.get_instropection(
        Document("insert into t1 VALUES (1,2", len("insert into t1 VALUES (1,2")),
        autocompleter.completer,
    )

    assert {
        "type": "column_hint",
        "hint": "",
        "table_name": "t1",
        "value_index": 1,
    } == result
    client.run_statement("drop database db1;")


def test_introspection_provider_introspect_insert_into_after_VALUES_would_suggest_column_hint_without_column_list(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table t1 (c int, b int, a int);")

    provider = IntrospectionProvider()
    autocompleter = Autocompleter(client, mocklog)
    autocompleter.refresh()

    result = provider.get_instropection(
        Document("insert into t1 VALUES (1,2,", len("insert into t1 VALUES (1,2,")),
        autocompleter.completer,
    )

    assert {
        "type": "column_hint",
        "hint": "",
        "table_name": "t1",
        "value_index": 2,
    } == result
    client.run_statement("drop database db1;")
