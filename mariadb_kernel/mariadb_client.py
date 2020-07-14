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
        result = lines[1]

        return result


class MariaDBClient:
    def __init__(self, log, config):
        client_bin = "/home/bindar/MariaDB/server/client/mariadb"
        kernel_args = "-s -H"
        args = config.get_args()
        self.cmd = f"{client_bin} {kernel_args} {args}"

        self.prompt = re.compile(r"MariaDB \[.*\]>[ \t]")
        self.log = log

    def start(self):
        try:
            self.maria_repl = MariaREPL(
                self.cmd,
                orig_prompt=self.prompt,
                prompt_change=None,
                continuation_prompt=None,
            )
            self.log.info("MariaDB client was started")
        except pexpect.ExceptionPexpect as e:
            self.log.error(f"MariaDB client failed to start: {e}")
            # TODO: attempt a restart and raise exception if it fails again

    def stop(self):
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

        return result
