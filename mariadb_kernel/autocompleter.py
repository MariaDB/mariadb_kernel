from logging import Logger
from typing import List
from mycli.packages.special.main import COMMANDS
from mariadb_kernel.sql_analyze import SQLAnalyze
from mariadb_kernel.sql_fetch import SqlFetch
from mariadb_kernel.mariadb_client import MariaDBClient
from prompt_toolkit.document import Document


class Refresher(object):
    def refresh_databases(self, completer: SQLAnalyze, executor: SqlFetch):
        completer.extend_database_names(executor.databases())

    def refresh_schemata(self, completer: SQLAnalyze, executor: SqlFetch):
        # schemata - In MySQL Schema is the same as database. But for mycli
        # schemata will be the name of the current database.
        completer.extend_schemata(executor.dbname)
        completer.set_dbname(executor.dbname)

    def refresh_tables(self, completer: SQLAnalyze, executor: SqlFetch):
        completer.extend_relations(executor.tables(), kind="tables")
        completer.extend_columns(executor.table_columns(), kind="tables")

    def refresh_users(self, completer: SQLAnalyze, executor: SqlFetch):
        completer.extend_users(executor.users())

    def refresh_functions(self, completer: SQLAnalyze, executor: SqlFetch):
        completer.extend_functions(executor.functions())

    def refresh_special(self, completer: SQLAnalyze, executor: SqlFetch):
        completer.extend_special_commands(COMMANDS.keys())

    def refresh_show_commands(self, completer: SQLAnalyze, executor: SqlFetch):
        completer.extend_show_items(executor.show_candidates())

    def refresh(self, executer: SqlFetch):
        completer = SQLAnalyze(True)
        self.refresh_databases(completer, executer)
        self.refresh_schemata(completer, executer)
        self.refresh_tables(completer, executer)
        self.refresh_users(completer, executer)
        self.refresh_functions(completer, executer)
        self.refresh_special(completer, executer)
        self.refresh_show_commands(completer, executer)

        executer.update_db_name()
        return completer


class Autocompleter(object):
    def __init__(self, mariadb_client: MariaDBClient, log: Logger) -> None:
        self.refresher = Refresher()
        self.executer = SqlFetch(mariadb_client, log)
        self.completer = self.refresher.refresh(self.executer)

    def refresh(self):
        self.completer = self.refresher.refresh(self.executer)

    def get_suggestions(self, code: str, cursor_pos: int) -> List[str]:
        self.refresh()
        result = self.completer.get_completions(
            document=Document(text=code, cursor_position=cursor_pos),
            complete_event=None,
            smart_completion=True,
        )
        str_list = [completion.text for completion in result]
        return str_list
