import os
import pytest
from unittest.mock import patch, Mock

from ..code_parser import CodeParser

import pdb


def test_parser_throws_when_no_delimiter_found():
    cell = "select 1"

    # CodeParser should throw ValueError when no delimiter is found in the code
    def assert_parser_throws():
        with pytest.raises(ValueError) as e:
            parser = CodeParser(Mock(), cell, ";")
        assert "Your SQL code doesn't" in str(e.value)

    assert_parser_throws()

    # Even thoough some_magic might look like a magic command, the
    # parser should look at it like part of the SQL statement
    cell = """
    select a from t
    %some_magic
    where a=2;
    """
    CodeParser(Mock(), cell, ";")


def test_parser_get_sql():
    sql = "select * from mysql.user;"
    linemagic = "%line_magic arg1 arg2"
    cellmagic = "%%delimiter"
    cellmagic_args = "arg1 arg2"

    # Single line SQL is parsed correctly
    parser = CodeParser(Mock(), sql, ";")
    statements = parser.get_sql()
    assert len(statements) == 1
    assert statements[0] == sql

    # Line magics are only permitted as singular in the cell,
    # everything else below the line magic cmd is ignored
    parser = CodeParser(Mock(), linemagic + "\n" + sql, ";")
    statements = parser.get_sql()
    magics = parser.get_magics()
    assert len(statements) == 0
    assert len(magics) == 1

    # Parser sees the cell magic and gets its inline arguments right,
    # and the rest of the cell is considered a code argument for the magic command
    parser = CodeParser(Mock(), cellmagic + " " + cellmagic_args + "\n" + sql, ";")
    statements = parser.get_sql()
    magics = parser.get_magics()
    assert len(statements) == 0
    assert len(magics) == 1
    assert magics[0].type() == "Cell"
    assert magics[0].name() == cellmagic
    assert str(magics[0].args) == str({"args": cellmagic_args, "code": sql})

    # Multi-line SQL
    sql = """select user,host from mysql.user
            where user='robert';"""
    parser = CodeParser(Mock(), sql, ";")
    statements = parser.get_sql()
    assert len(statements) == 1
    assert statements[0] == sql
