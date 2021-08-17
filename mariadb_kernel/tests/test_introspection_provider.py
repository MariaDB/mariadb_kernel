from typing import Type

from ..kernel import MariadbClientManagager

from ..mariadb_server import MariaDBServer
from ..client_config import ClientConfig
from ..introspector import Introspector
from ..autocompleter import Autocompleter

from unittest.mock import Mock
import pytest


def test_introspection_provider_introspect_keyword(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection(
        "select col1 from b;", len("sele"), autocompleter
    )

    assert {"type": "keyword", "word": "select"} == result


def test_introspection_provider_introspect_function(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection(
        "select min(col1) from b;", len("select mi"), autocompleter
    )

    assert {"type": "function", "word": "min"} == result


def test_introspection_provider_introspect_database(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database mydb;")
    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection("use mydb;", len("use my"), autocompleter)

    assert {"type": "database", "word": "mydb"} == result
    client.run_statement("drop database mydb;")


def test_introspection_provider_introspect_table(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database mydb;")
    client.run_statement("use mydb;")
    client.run_statement("create table tbl1 (col1 int);")
    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection(
        "select col1 from tbl1",
        len("select col1 from tbl"),
        autocompleter,
    )

    assert {"type": "table", "word": "tbl1", "database": "mydb"} == result
    client.run_statement("drop database mydb;")


def test_introspection_provider_introspect_table_that_is_not_belong_current_use_db(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database db1;")
    client.run_statement("create database db2;")
    client.run_statement("use db2;")
    client.run_statement("create table tbl1 (col1 int);")
    client.run_statement("use db1;")
    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection(
        "insert into db2.tbl1 ",
        len("insert into db2.t"),
        autocompleter,
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

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table tbl1 (col1 int);")

    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection(
        "select col1 from tbl1", len("select col"), autocompleter
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

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table tbl1 (col1 int);")

    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection(
        "select col1 from ", len("select col"), autocompleter
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

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table tbl1 (min int);")

    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection(
        "select min from tbl1;", len("select mi"), autocompleter
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

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table tbl1 (type int);")

    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection(
        "select type from tbl1;", len("select ty"), autocompleter
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

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table tbl1 (min int);")

    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection(
        "select min(min) from tbl1;",
        len("select mi"),
        autocompleter,
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

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table min (col1 int);")
    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection(
        "select * from min;", len("select * from m"), autocompleter
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

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table type (col1 int);")

    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection(
        "select * from type;",
        len("select * from ty"),
        autocompleter,
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

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table col_tabl (col1 int);")
    client.run_statement("create table tabl2 (col_tabl int);")

    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection(
        "select tabl2.col_tabl from tabl2;",
        len("select tabl2.col_"),
        autocompleter,
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

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table t1 (a int, b int, c int);")

    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection(
        "insert into t1 (a, b, c) VALUES (",
        len("insert into t1 (a, b, c) VALUES ("),
        autocompleter,
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

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table t1 (a int, b int, c int);")

    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection(
        input,
        len(input),
        autocompleter,
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

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table t1 (a int);")

    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection(
        "insert into t1 (a) VALUES (1,2",
        len("insert into t1 (a) VALUES (1,2"),
        autocompleter,
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

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table t1 (a int);")

    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection(
        "insert into t1 VALUES (1,2",
        len("insert into t1 VALUES (1,2"),
        autocompleter,
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

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table t1 (c int, b int, a int);")

    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection(
        "insert into t1 VALUES (1,2,",
        len("insert into t1 VALUES (1,2,"),
        autocompleter,
    )

    assert {
        "type": "column_hint",
        "hint": "",
        "table_name": "t1",
        "value_index": 2,
    } == result
    client.run_statement("drop database db1;")


def test_introspection_provider_introspect_insert_into_after_VALUES_would_suggest_column_hint_for_multi_value_list(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table t1 (c int, b int, a int);")

    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection(
        "insert into t1 VALUES (1,2), (3,4), (5,",
        len("insert into t1 VALUES (1,2), (3,4), (5,"),
        autocompleter,
    )

    assert {
        "type": "column_hint",
        "hint": "",
        "table_name": "t1",
        "value_index": 1,
    } == result
    client.run_statement("drop database db1;")


def test_introspection_provider_introspect_column_after_user_column(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")

    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection(
        "select user, password from mysql.user;",
        len("select user, passwo"),
        autocompleter,
    )

    assert {
        "type": "column",
        "word": "password",
        "table": "user",
        "database": "mysql",
    } == result
    client.run_statement("drop database db1;")


def test_introspection_provider_introspect_user_column_in_system_table(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")

    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection(
        "select user from mysql.user;",
        len("select use"),
        autocompleter,
    )

    assert {
        "type": "column",
        "word": "user",
        "table": "user",
        "database": "mysql",
    } == result
    client.run_statement("drop database db1;")


def test_introspection_provider_introspect_user_column_in_user_create_table(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    client.run_statement("create table t1(user varchar(20));")

    introspector = Introspector()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    result = introspector.get_instropection(
        "select user from t1;",
        len("select use"),
        autocompleter,
    )

    assert {
        "type": "column",
        "word": "user",
        "table": "t1",
        "database": "db1",
    } == result
    client.run_statement("drop database db1;")
