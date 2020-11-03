"""The code that wraps a MariaDB command line client"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from pexpect import replwrap, EOF, TIMEOUT, ExceptionPexpect
import re


class MariaREPL(replwrap.REPLWrapper):
    def __init__(self, *args, **kwargs):
        replwrap.REPLWrapper.__init__(self, *args, **kwargs)
        self.args = args
        self.kwargs = kwargs

    def _expect_prompt(self, timeout=-1, async_=False):
        return self.child.expect([self.prompt], timeout=timeout, async_=async_)

    def run_command(self, code, timeout=-1, async_=False):
        # TODO: take care of pexpect.send limitation of 1024 characters
        # TODO: clean input if comments are permitted
        # TODO: implement support for continuation prompt

        self.child.sendline(code)
        self._expect_prompt(timeout, async_)

        lines = self.child.before.split("\r\n")

        # In pre-10.5 versions, silent mode doesn't print the SQL statement
        # before the result set
        if code.strip() == lines[0].strip():
            return lines[1]

        return lines[0]


class MariaDBClient:
    def __init__(self, log, config):
        self.maria_repl = None
        self.client_bin = config.client_bin()
        kernel_args = "-s -H"
        args = config.get_args()
        self.cmd = f"{self.client_bin} {kernel_args} {args}"

        self.prompt = re.compile(r"MariaDB \[.*\]>[ \t]")
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
        except EOF as e:
            self.log.error("MariaDB client failed to start")

            if "Access denied for user" in e.value:
                self.log.error("The credentials used for connecting are wrong")
                raise LoginError()

            self.log.error("Most probably the MariaDB server is not started")

            # Let the kernel know the server is down
            raise ServerIsDownError()
        except ExceptionPexpect as e:
            self.log.error("No mariadb> command line client found at "
                           f"{self.client_bin};")
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
        #TODO: double check exception handling
        try:
            result = self.maria_repl.run_command(code, timeout)
        except EOF as e:
            self.log.error(
                f'MariaDB client failed to run command "{code}". '
                f"Client most probably exited due to a crash: {e}"
            )
            # TODO: attempt a restart and raise exception if it fails again
        except TIMEOUT as e:
            self.log.error(
                f'MariaDB client failed to run command "{code}". '
                f"Reading from the client timed out: {e}"
            )
            # TODO: attempt to rerun the cmd and raise exception if failure

        if result.startswith('ERROR'):
            self.error = True
            self.errormsg = result
        else:
            self.error = False

        return result

class ServerIsDownError(Exception):
    pass

class LoginError(Exception):
    pass
