import enum
from typing import Callable, List, Tuple
import pandas
from pandas.core.frame import DataFrame
from mariadb_kernel.mariadb_client import MariaDBClient
import logging
import math


class SqlFetch:
    def __init__(self, maridb_client: MariaDBClient, log: logging.Logger) -> None:
        self.maridb_client = maridb_client
        self.log = log
        self.update_db_name()

    def fetch_info(self, query: str, function: Callable[[List[DataFrame]], List]):
        result_html = self.maridb_client.run_statement(query)
        if self.maridb_client.iserror():
            raise Exception(f"Client returned an error : {result_html}")
        try:
            if result_html == "Query OK":
                str_list = []
            else:
                df = pandas.read_html(result_html)
                str_list = function(df)
        except Exception:
            self.log.error(f"Pandas failed to parse html : {result_html}")
            raise
        return str_list

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
        show_candidates_query = """SELECT name from mysql.help_topic
                                               WHERE name like "SHOW %";"""

        # remove show prefix
        return [
            (name.split(None, 1)[-1],)
            for name in self.fetch_info(
                show_candidates_query,
                lambda df: list(df[0][df[0].columns[0]].values),
            )
        ]

    def users(self):
        # users_query
        users_query = """SELECT CONCAT("'", user, "'@'",host,"'")
                                FROM mysql.user;"""
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
        functions_query = """SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES
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
        result_html = self.maridb_client.run_statement(
            table_columns_query % self.dbname
        )
        if self.maridb_client.iserror():
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
        fetch_keywords_query = "select word from information_schema.keywords;"
        try:
            result = self.fetch_info(
                fetch_keywords_query, lambda df: list(df[0]["word"])
            )
            return list(filter(lambda ele: type(ele) is not float, result))
        except Exception as e:
            return []

    def sql_functions(self) -> List[str]:
        # need consider no information_schema.keywords table
        fetch_keywords_query = "select function from information_schema.sql_functions;"
        try:
            result = self.fetch_info(
                fetch_keywords_query, lambda df: list(df[0]["function"])
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
        database_tables_query = (
            """select TABLE_SCHEMA, TABLE_NAME from information_schema.TABLES"""
        )
        result_html = self.maridb_client.run_statement(database_tables_query)
        if self.maridb_client.iserror():
            raise Exception(f"Client returned an error : {result_html}")
        try:
            if result_html == "Query OK":
                return []
            df = pandas.read_html(result_html)
            database_name_list = list(df[0]["TABLE_SCHEMA"].values)
            table_name_list = list(df[0]["TABLE_NAME"].values)
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
        session_variables_query = "show global VARIABLES;"
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

    def change_db_name(self, db):
        self.dbname = db

    def update_db_name(self):
        self.dbname = self.get_db_name()
