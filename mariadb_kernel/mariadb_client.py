"""The code that wraps a MariaDB command line client"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

import re
from pathlib import Path
from uuid import uuid1
from bs4 import BeautifulSoup
from pexpect import replwrap, EOF, TIMEOUT, ExceptionPexpect


class MariaREPL(replwrap.REPLWrapper):
    def __init__(self, *args, **kwargs):
        replwrap.REPLWrapper.__init__(self, *args, **kwargs)
        self.args = args
        self.kwargs = kwargs

    def _expect_prompt(self, timeout=-1, async_=False):
        patterns = [self.prompt]
        return self.child.expect(patterns, timeout=timeout, async_=async_)

    def run_command(self, command, timeout=-1, async_=False):

        # Writing the cell code within a file and then sourcing it in the client
        # offers us a lot of advantages.
        # We avoid Pexpect's limitation of PC_MAX_CANON (1024) chars per line
        # and we also avoid more nasty issues like MariaDB client behaviour
        # sending continuation prompt when "\n" is received.
        stmt_file = ".mariadb_statement" + "_" + str(uuid1())
        statement_file_path = Path.cwd().joinpath(stmt_file)
        with statement_file_path.open("w", encoding="utf-8") as file:
            file.write(command)
        self.child.sendline(f"source {str(statement_file_path)}")

        try:
            self._expect_prompt(timeout, async_)
        finally:
            statement_file_path.unlink()

        return self.child.before


class MariaDBClient:
    def __init__(self, log, config):
        self.maria_repl = None
        self.client_bin = config.client_bin()
        kernel_args = "-s -H"
        args = config.get_args()
        self.cmd = f"{self.client_bin} {kernel_args} {args}"

        self.prompt = re.compile(config.server_name() + r" \[.*\]>[ \t]")
        self.log = log
        self.error = False
        self.errormsg = ""

    def iserror(self):
        return self.error

    def error_message(self):
        return self.errormsg

    def _launch_client(self):
        self.maria_repl = MariaREPL(
            self.cmd,
            orig_prompt=self.prompt,
            prompt_change=None,
            continuation_prompt=None,
        )

    def start(self):
        try:
            self._launch_client()
            self.log.info("MariaDB client was successfully started")
        except EOF as exception:
            self.log.error("MariaDB client failed to start")

            if "Access denied for user" in exception.value:
                self.log.error("The credentials used for connecting are wrong")
                raise LoginError from exception

            self.log.error("Most probably the MariaDB server is not started")

            # Let the kernel know the server is down
            raise ServerIsDownError from exception
        except ExceptionPexpect:
            self.log.error(
                "No mariadb> command line client found at " f"{self.client_bin};"
            )
            self.log.error("Please install MariaDB from mariadb.org/download")

    def stop(self):
        if self.maria_repl is None:
            return

        # pexpect will always raise EOF because the mariadb client exits,
        # better we just expect it
        self.maria_repl.child.sendline("quit")
        self.maria_repl.child.expect(EOF)
        self.log.info("MariaDB client was successfully stopped")

    def run_statement(self, code, timeout=-1):
        if not code:
            return ""

        result = ""
        try:
            result = self.maria_repl.run_command(code, timeout)
        except EOF as exception:
            self.log.error(
                f'MariaDB client failed to run command "{code}". '
                f"Client most probably exited due to a crash: {exception}"
            )
            # TODO: attempt a restart and raise exception if it fails again
        except TIMEOUT as exception:
            self.log.error(
                f'MariaDB client failed to run command "{code}". '
                f"Reading from the client timed out: {exception}"
            )
            # TODO: attempt to rerun the cmd and raise exception if failure

        if result.startswith("ERROR"):
            self.error = True

            # Get rid of extra info in the error message that isn't interesting
            # in this case.
            # This matches error messages that look like:
            # """ERROR 1064 (42000) at line 1 in file:
            # './mariadb_statement': You have an error..."""
            # We only keep the SQL error message and discard the first part
            regex = re.compile(r"^ERROR.+in file: \'.+mariadb_statement.+\': ")
            self.errormsg = regex.sub("", result, count=1)
            return self.errormsg
        if not result:
            result = "Query OK"

        self.error = False
        return result

    def styled_result(self, result_html):
        if not result_html or not result_html.startswith("<TABLE"):
            return result_html

        soup = BeautifulSoup(result_html)
        cells = soup.find_all(["td", "th"])
        for cell in cells:
            cell["style"] = "text-align:left;white-space:pre"

        table = soup.find("table")
        table["style"] = "margin-left: 0"

        return str(soup)


class ServerIsDownError(Exception):
    pass


class LoginError(Exception):
    pass
