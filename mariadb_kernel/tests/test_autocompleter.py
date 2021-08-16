from typing import List, Type

from prompt_toolkit.completion.base import Completion
from ..kernel import MariadbClientManagager

from ..mariadb_server import MariaDBServer
from ..client_config import ClientConfig

from unittest.mock import Mock
from ..autocompleter import Autocompleter

import unittest


def get_text_list(completions: List[Completion]):
    return [completion.text for completion in completions]


def get_text_list_by_type(completions: List[Completion], type: str):
    return [
        completion.text
        for completion in completions
        if completion.display_meta_text == type
    ]


def test_mariadb_autocompleter_keywords_suggestion(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("use test;")
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    assert set(["select"]).issubset(
        get_text_list(autocompleter.get_suggestions("sel", 3))
    )


def test_mariadb_autocompleter_default_functions_suggestion(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("use test;")
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    assert set(autocompleter.completer.functions).issubset(
        get_text_list(autocompleter.get_suggestions("select ", 7))
    )


def test_mariadb_autocompleter_database_suggestion_after_use(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("use test;")
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    assert set(["information_schema", "mysql", "performance_schema", "test"]).issubset(
        get_text_list(autocompleter.get_suggestions("use ", 4))
    )


def test_mariadb_autocompleter_table_suggestion_under_database_by_dot(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("use test;")
    client.run_statement("create table t1 (a int, b int, c int);")
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)

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

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("use test;")
    client.run_statement("create table t1 (a int, b int, c int);")
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)

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

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("use test;")
    client.run_statement("create table t1 (a int, b int, c int);")
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)

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

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("use test;")
    client.run_statement("create table t1 (a int, b int, c int);")
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)

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

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("use test;")
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)

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

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("use test;")
    client.run_statement("create user foo2@test IDENTIFIED BY 'password';")
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)

    # not complete
    assert set(
        [
            "'root'@'localhost'",
            "'foo2'@'test'",
        ]
    ).issubset(get_text_list(autocompleter.get_suggestions("ALTER USER ", 11)))

    client.run_statement("drop foo2@test;")


def test_mariadb_autocompleter_database_before_table_name(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database d1;")
    client.run_statement("use d1;")
    client.run_statement("create table t1(a int, b int);")
    client.run_statement("create table t2(a int, b int);")
    client.run_statement("create database d2;")
    client.run_statement("use d2;")
    client.run_statement("create table haha1(a int, b int);")
    client.run_statement("create table haha2(a int, b int);")
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    unittest.TestCase().assertListEqual(
        ["d2"],
        get_text_list(
            autocompleter.get_suggestions("insert into .haha1", len("insert into "))
        ),
    )

    client.run_statement("drop database d1;")
    client.run_statement("drop database d2;")


def test_mariadb_autocompleter_database_before_table_name_under_emtpy_table_name(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database d1;")
    client.run_statement("use d1;")
    client.run_statement("create table t1(a int, b int);")
    client.run_statement("create table t2(a int, b int);")
    client.run_statement("create database d2;")
    client.run_statement("use d2;")
    client.run_statement("create table haha1(a int, b int);")
    client.run_statement("create table haha2(a int, b int);")
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    set(["d1", "d2"]).issubset(
        get_text_list(
            autocompleter.get_suggestions("insert into .", len("insert into "))
        ),
    )

    client.run_statement("drop database d1;")
    client.run_statement("drop database d2;")


def test_mariadb_autocompleter_database_before_table_name_under_partial_database_name(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    client.run_statement("create database da1;")
    client.run_statement("use da1;")
    client.run_statement("create table t1(a int, b int);")
    client.run_statement("create table t2(a int, b int);")
    client.run_statement("create database db2;")
    client.run_statement("use db2;")
    client.run_statement("create table haha1(a int, b int);")
    client.run_statement("create table haha2(a int, b int);")
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    unittest.TestCase().assertListEqual(
        ["da1"],
        get_text_list(
            autocompleter.get_suggestions("insert into da.", len("insert into da"))
        ),
    )
    client.run_statement("drop database da1;")
    client.run_statement("drop database db2;")


def test_mariadb_autocompleter_variables_suggestion_with_empty_text(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    # check no duplicate
    assert (
        len(
            list(
                filter(
                    lambda text: text == "character_set_database",
                    get_text_list(
                        autocompleter.get_suggestions("select @@", len("select @@"))
                    ),
                )
            )
        )
        < 2
    )
    # jsut test part of variable
    set(
        [
            "character_set_connection",
            "datadir",
            "date_format",
            "extra_max_connections",
            "max_error_count",
            "time_zone",
        ]
    ).issubset(
        get_text_list(autocompleter.get_suggestions("select @@", len("select @@")))
    )


def test_mariadb_autocompleter_variables_suggestion_with_some_text(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()

    # jsut test part of variable
    unittest.TestCase().assertListEqual(
        [
            "query_alloc_block_size",
            "query_cache_limit",
            "query_cache_min_res_unit",
            "query_cache_size",
            "query_cache_strip_comments",
            "query_cache_type",
            "query_cache_wlock_invalidate",
            "query_prealloc_size",
            "ft_query_expansion_limit",
            "have_query_cache",
            "long_query_time",
            "slow_query_log",
            "slow_query_log_file",
            "expensive_subquery_limit",
        ],
        get_text_list(
            autocompleter.get_suggestions("select @@query_", len("select @@query_"))
        ),
    )


def test_mariadb_autocompleter_global_variables_suggestion(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()
    # jsut test part of variable
    unittest.TestCase().assertListEqual(
        ["report_host"],
        get_text_list(
            autocompleter.get_suggestions(
                "select @@global.report_h", len("select @@global.report_h")
            )
        ),
    )


def test_mariadb_autocompleter_global_variables_no_session_variable(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()
    # jsut test part of variable
    unittest.TestCase().assertNotIn(
        "error_count",
        get_text_list(
            autocompleter.get_suggestions("select @@global.", len("select @@global."))
        ),
    )


def test_mariadb_autocompleter_global_variables_no_session_variable_with_text(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    autocompleter.refresh()
    # jsut test part of variable
    unittest.TestCase().assertNotIn(
        "error_count",
        get_text_list(
            autocompleter.get_suggestions(
                "select @@global.err", len("select @@global.err")
            )
        ),
    )


def test_mariadb_autocompleter_multi_mariadb_client_selected_database_sync(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    autocompleter.refresh()
    assert manager.client_for_autocompleter.run_statement(
        "SELECT DATABASE();"
    ) == manager.client_for_code_block.run_statement("SELECT DATABASE();")


def test_mariadb_autocompleter_column_suggest_for_system_table(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    autocompleter.refresh()
    assert ["Password", "password_expired"] == get_text_list_by_type(
        autocompleter.get_suggestions(
            "select user, Passwor from mysql.user;", len("select user, Passwor")
        ),
        "column",
    )


def test_mariadb_autocompleter_column_suggest_for_system_table_2(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    autocompleter.refresh()
    assert ["Select_priv"] == get_text_list_by_type(
        autocompleter.get_suggestions(
            "select user, password, Select_priv from mysql.user;",
            len("select user, password, Select_pri"),
        ),
        "column",
    )


def test_mariadb_autocomple_actively_fetch_system_table_columns(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    manager = MariadbClientManagager(mocklog, cfg)
    client = manager.client_for_code_block
    manager.start()
    autocompleter = Autocompleter(manager.client_for_autocompleter, client, mocklog)
    client.run_statement("create database db1;")
    client.run_statement("use db1;")
    autocompleter.refresh()
    assert ["TABLE_NAME"] == get_text_list_by_type(
        autocompleter.get_suggestions(
            "select TABLE_NAM from information_schema.TABLES;", len("select TABLE_NAM")
        ),
        "column",
    )
