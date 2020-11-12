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
            parser = CodeParser(Mock(), cell)
        assert "No delimiter" in str(e.value)

    assert_parser_throws()

    # Finding a magic in the middle of the sentence should throw an exception,
    # it is not allowed
    cell = """
    select a from t
    %some_magic
    where a=2;
    """
    assert_parser_throws()

    # This shouldn't throw because parser should see the last two lines as
    # a multi-line SQL code
    cell = """
    select 1;
    select 1
    select 1;
    """
    CodeParser(Mock(), cell)

def test_parser_get_sql():
    sql = "select * from mysql.user; "
    linemagic = "%line_magic"
    cellmagic = "%%cell_magic"

    # Single line SQL is parsed correctly
    parser = CodeParser(Mock(), sql)
    statements = parser.get_sql()
    assert len(statements) == 1
    assert statements[0] == sql

    # Magics and SQL in the same cell are permited and they should be parsed correctly
    parser = CodeParser(Mock(), sql + "\n" + linemagic + "\n" + sql + "\n" + cellmagic)
    statements = parser.get_sql()
    magics = parser.get_magics()
    assert len(statements) == 2
    assert len(magics) == 2
    assert statements == [sql, sql]

    # Multi-line SQL
    sql = """
    select user,host from mysql.user
    where user='robert';
    """
    parser = CodeParser(Mock(), sql)
    statements = parser.get_sql()
    assert len(statements) == 1
    assert statements[0] == "select user,host from mysql.user where user='robert'; "

