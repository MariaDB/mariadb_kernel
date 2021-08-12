from logging import Logger
import threading
from typing import Callable, List
from mycli.packages.special.main import COMMANDS
from mariadb_kernel.sql_analyze import SQLAnalyze
from mariadb_kernel.sql_fetch import SqlFetch
from mariadb_kernel.mariadb_client import MariaDBClient
from prompt_toolkit.document import Document
from threading import Thread


class Refresher(object):
    def __init__(self, completer: SQLAnalyze, executor: SqlFetch, log: Logger) -> None:
        self.executor = executor
        self.fetch_keywords = self.executor.keywords()
        self.fetch_functions = self.executor.sql_functions()
        self.log = log
        self.refresh_complete = True
        self.refresh_thread = None
        self.old_completer = completer

    def refresh_databases(self):
        self.completer.extend_database_names(self.executor.databases())

    def refresh_schemata(self):
        # schemata - In MySQL Schema is the same as database. But for mycli
        # schemata will be the name of the current database.
        self.completer.extend_schemata(self.executor.dbname)
        self.completer.set_dbname(self.executor.dbname)

    def refresh_tables(self):
        self.completer.extend_relations(self.executor.tables(), kind="tables")
        self.completer.extend_columns(self.executor.table_columns(), kind="tables")

    def refresh_users(self):
        self.completer.extend_users(self.executor.users())

    def refresh_functions(self):
        self.completer.extend_functions(self.executor.functions())

    def refresh_special(self):
        self.completer.extend_special_commands(COMMANDS.keys())

    def refresh_show_commands(self):
        self.completer.extend_show_items(self.executor.show_candidates())

    def refresh_database_tables(self):
        self.completer.extend_tables(self.executor.database_tables())

    def refresh_variables(self):
        self.completer.extend_global_variables(self.executor.global_variables())
        self.completer.extend_session_variables(self.executor.session_variables())

    def refresh_all(self):
        self.refresh_complete = False
        self.completer = SQLAnalyze(self.log, True)
        refresh_func_list: List[Callable] = [
            self.refresh_databases,
            self.refresh_schemata,
            self.refresh_tables,
            self.refresh_users,
            self.refresh_functions,
            self.refresh_special,
            self.refresh_show_commands,
            self.refresh_database_tables,
            self.refresh_variables,
        ]
        for refresh_func in refresh_func_list:
            refresh_func()
        self.completer.set_keywords(self.fetch_keywords)
        self.completer.set_functions(self.fetch_functions)
        self.refresh_complete = True
        self.old_completer.reset_completions(self.completer)

    def refresh(self, sync=False):
        if sync:
            self.refresh_all()
        else:
            if self.refresh_complete is True:
                self.refresh_thread = Thread(target=self.refresh_all)
                self.refresh_thread.start()


class Autocompleter(object):
    def __init__(
        self,
        mariadb_client: MariaDBClient,
        code_block_mariadb_client: MariaDBClient,
        log: Logger,
    ) -> None:
        self.log = log
        self.autocompleter_mariadb_client = mariadb_client
        self.executor = SqlFetch(mariadb_client, log)
        self.code_blcok_executor = SqlFetch(code_block_mariadb_client, log)
        self.completer = SQLAnalyze(log, True)
        self.refresher = Refresher(self.completer, self.executor, log)
        self.refresh()

    def refresh(self, sync: bool = True):
        code_block_db_name = self.code_blcok_executor.get_db_name()
        if self.executor.dbname != code_block_db_name and code_block_db_name != "":
            self.autocompleter_mariadb_client.run_statement(f"use {code_block_db_name}")
            self.executor.dbname = self.code_blcok_executor.get_db_name()
        self.refresher.refresh(sync)
        self.log.info(f"self.completer.dbname : {self.completer.dbname}")
        self.log.info(f"self.completer.dbmetadata : {self.completer.dbmetadata}")

    def get_suggestions(self, code: str, cursor_pos: int):
        # self.refresh()
        result = self.completer.get_completions(
            document=Document(text=code, cursor_position=cursor_pos),
            complete_event=None,
            smart_completion=True,
        )
        return list(result)
