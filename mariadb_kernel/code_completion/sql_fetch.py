from collections import namedtuple
import enum
from typing import Callable, List, NamedTuple, Tuple
import pandas
from pandas.core.frame import DataFrame
from mariadb_kernel.mariadb_client import MariaDBClient
import logging
import math


class SqlFetch:
    def __init__(self, mariadb_client: MariaDBClient, log: logging.Logger) -> None:
        self.mariadb_client = mariadb_client
        self.log = log
        self.update_db_name()

    def fetch_info(
        self, query: str, function: Callable[[List[DataFrame]], List], html=False
    ):
        result_html = self.mariadb_client.run_statement(query)
        rv = None
        if self.mariadb_client.iserror():
            raise Exception(f"Client returned an error : {result_html}")
        try:
            if result_html == "Query OK":
                if html:
                    rv = ""
                else:
                    rv = []
            elif html:
                rv = result_html
            else:
                df = pandas.read_html(result_html)
                rv = function(df)
        except Exception:
            self.log.error(f"Pandas failed to parse html : {result_html}")
            raise
        return rv

    def databases(self) -> List[str]:
        databases_query = """SHOW DATABASES;"""
        return self.fetch_info(
            databases_query, lambda df: list(df[0]["Database"].values)
        )

    def tables(self):
        # tables_query
        tables_query = """SHOW TABLES;"""
        if self.dbname == "":
            return []
        return [
            (name,)
            for name in self.fetch_info(
                tables_query, lambda df: list(df[0][df[0].columns[0]].values)
            )
        ]

    def show_candidates(self):
        # show_candidates_query
        show_candidates_query = """SELECT lower(name) from mysql.help_topic
                                                      WHERE name like "SHOW %";"""

        # remove show prefix
        return [
            (name.split(None, 1)[-1],)
            for name in self.fetch_info(
                show_candidates_query,
                lambda df: list(df[0][df[0].columns[0]].values),
            )
        ]

    def users(self, html=False):
        # users_query
        users_query = """SELECT CONCAT("'", user, "'@'",host,"'") as All_Users
                                FROM mysql.user;"""

        if html:
            users = self.fetch_info(users_query, None, html=True)
            return self.mariadb_client.styled_result(users)

        return [
            (name,)
            for name in self.fetch_info(
                users_query, lambda df: list(df[0][df[0].columns[0]].values)
            )
        ]

    def functions(self):
        # functions_query
        if self.dbname == "":
            return []
        functions_query = """SELECT lower(ROUTINE_NAME) FROM INFORMATION_SCHEMA.ROUTINES
                                                        WHERE ROUTINE_TYPE="FUNCTION"
                                                        AND ROUTINE_SCHEMA = "%s";"""
        return [
            (name,)
            for name in self.fetch_info(
                functions_query % self.dbname,
                lambda df: list(df[0][df[0].columns[0]].values),
            )
        ]

    def table_columns(self) -> List[Tuple[str, str]]:
        # table_columns_query
        if self.dbname == "":
            return []
        table_columns_query = """select TABLE_NAME, COLUMN_NAME from information_schema.columns
                                                                where table_schema = '%s'
                                                                order by table_name,ordinal_position;"""
        result_html = self.mariadb_client.run_statement(
            table_columns_query % self.dbname
        )
        if self.mariadb_client.iserror():
            raise Exception(f"Client returned an error : {result_html}")
        try:
            if result_html == "Query OK":
                return []
            df = pandas.read_html(result_html)
            table_name_list = list(df[0]["TABLE_NAME"].values)
            column_name_list = list(df[0]["COLUMN_NAME"].values)
            table_column_list = [
                (table_name_list[i], column_name_list[i])
                for i, _ in enumerate(table_name_list)
            ]
        except Exception:
            self.log.error(f"Pandas failed to parse html : {result_html}")
            raise
        return table_column_list

    def keywords(self) -> List[str]:
        # need consider no information_schema.keywords table
        fetch_keywords_query = "select lower(word) from information_schema.keywords;"
        try:
            result = self.fetch_info(
                fetch_keywords_query, lambda df: list(df[0]["lower(word)"])
            )
            return list(filter(lambda ele: type(ele) is not float, result))
        except Exception as e:
            return []

    def sql_functions(self) -> List[str]:
        # need consider no information_schema.keywords table
        fetch_keywords_query = (
            "select lower(function) from information_schema.sql_functions;"
        )
        try:
            result = self.fetch_info(
                fetch_keywords_query, lambda df: list(df[0]["lower(function)"])
            )
            return list(filter(lambda ele: type(ele) is not float, result))
        except Exception as e:
            return []

    def num_connected_clients(self):
        connected_client_num_query = "show status like 'Threads_connected';"
        str_list = self.fetch_info(
            connected_client_num_query, lambda df: list(df[0]["Value"].values)
        )
        return int(str_list[0])

    #     database_tables_query = """select TABLE_SCHEMA, TABLE_NAME from information_schema.TABLES"""
    def database_tables(self):
        """Yields (database name, table name) pairs"""
        database_tables_query = """select lower(TABLE_SCHEMA), lower(TABLE_NAME) from information_schema.TABLES"""
        result_html = self.mariadb_client.run_statement(database_tables_query)
        if self.mariadb_client.iserror():
            raise Exception(f"Client returned an error : {result_html}")
        try:
            if result_html == "Query OK":
                return []
            df = pandas.read_html(result_html)
            database_name_list = list(df[0]["lower(TABLE_SCHEMA)"].values)
            table_name_list = list(df[0]["lower(TABLE_NAME)"].values)
            database_table_list = [
                (database_name_list[i], table_name_list[i])
                for i, _ in enumerate(database_name_list)
            ]
        except Exception:
            self.log.error(f"Pandas failed to parse html : {result_html}")
            raise
        return database_table_list

    def global_variables(self):
        global_variables_query = "show global VARIABLES;"
        return self.fetch_info(
            global_variables_query, lambda df: list(df[0]["Variable_name"])
        )

    def session_variables(self):
        session_variables_query = "show session VARIABLES;"
        return self.fetch_info(
            session_variables_query, lambda df: list(df[0]["Variable_name"])
        )

    def get_db_name(self):
        # functions_query
        current_use_database_query = "SELECT DATABASE();"
        result = self.fetch_info(
            current_use_database_query,
            lambda df: list(df[0][df[0].columns[0]].values),
        )[0]
        if type(result) != str:
            if math.isnan(result):
                return ""
        return result

    def get_tables_in_db_html(self, db: str):
        tables_from_db_query = f"show tables from {db}"
        result_html = self.mariadb_client.run_statement(tables_from_db_query)
        if self.mariadb_client.iserror():
            raise Exception(f"Client returned an error : {result_html}")
        if result_html == "Query OK":
            result_html = ""
        return self.mariadb_client.styled_result(result_html)

    def get_table_schema_html(self, table: str, db: str):
        table_schema_query = f"describe {db}.{table}"
        result_html = self.mariadb_client.run_statement(table_schema_query)
        if self.mariadb_client.iserror():
            raise Exception(f"Client returned an error : {result_html}")
        if result_html == "Query OK":
            result_html = ""
        return self.mariadb_client.styled_result(result_html)

    def get_partial_table_row_html(self, table: str, db: str, limit: int = 5):
        table_rows_query = f"select * from {db}.{table} limit {limit}"
        result_html = self.mariadb_client.run_statement(table_rows_query)
        if self.mariadb_client.iserror():
            raise Exception(f"Client returned an error : {result_html}")
        if result_html == "Query OK":
            result_html = ""
        return self.mariadb_client.styled_result(result_html)

    def get_column_type_html(self, column: str, table: str, db: str):
        column_type_query = f"""SELECT COLUMN_TYPE as Datatype
                                FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE
                                    TABLE_SCHEMA = '{db}' AND
                                    TABLE_NAME = '{table}' AND
                                    COLUMN_NAME = '{column}';"""
        result_html = self.mariadb_client.run_statement(column_type_query)
        if self.mariadb_client.iserror():
            raise Exception(f"Client returned an error : {result_html}")
        if result_html == "Query OK":
            result_html = ""
        return self.mariadb_client.styled_result(result_html)

    class ColumnType(NamedTuple):
        name: str
        type: str

    def get_column_type_list(self, table: str, db: str) -> List[ColumnType]:
        column_type_list_query = f"""SELECT lower(COLUMN_NAME), COLUMN_TYPE
                                     FROM INFORMATION_SCHEMA.COLUMNS
                                     WHERE
                                         TABLE_SCHEMA = '{db}' AND
                                         TABLE_NAME = '{table}'
                                     ORDER BY ORDINAL_POSITION;"""
        result_html = self.mariadb_client.run_statement(column_type_list_query)
        if self.mariadb_client.iserror():
            raise Exception(f"Client returned an error : {result_html}")

        if result_html == "Query OK":
            return []
        else:
            ColumnType = namedtuple("ColumnType", ["name", "type"])
            column_type_list = []
            df = pandas.read_html(result_html)
            pandas_table = df[0]
            for i, column_name in enumerate(pandas_table["lower(COLUMN_NAME)"]):
                column_type = ColumnType(column_name, pandas_table["COLUMN_TYPE"][i])
                column_type_list.append(column_type)
            return column_type_list

    def get_help_text(self, name: str) -> str:
        help_text_query = f"""help '{name}';"""
        text = self.mariadb_client.run_statement(help_text_query)
        if text == "Query OK":
            text = ""
        return text

    def get_column_row_html(self, column: str, table: str, db: str, limit: int = 5):
        column_rows_query = f"select {column} from {db}.{table} limit {limit}"
        result_html = self.mariadb_client.run_statement(column_rows_query)
        if self.mariadb_client.iserror():
            raise Exception(f"Client returned an error : {result_html}")
        if result_html == "Query OK":
            result_html = ""
        return self.mariadb_client.styled_result(result_html)

    def get_specific_table_columns_list(self, table: str, db: str):
        column_query = f"explain {db}.{table}"
        return self.fetch_info(column_query, lambda df: list(df[0]["Field"]))

    def change_db_name(self, db):
        self.dbname = db

    def update_db_name(self):
        self.dbname = self.get_db_name()
