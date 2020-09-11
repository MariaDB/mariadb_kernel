"""Abstract class that represents a magic command"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.


class MariaMagic:
    def execute(self, kernel, data):
        """Executes a magic command.
        **Subclasses must define this method**
        Args: kernel
              data
        """
        raise NotImplementedError()

    def type(self):
        raise NotImplementedError()

    def name(self):
        raise NotImplementedError()

    def help(self):
        raise NotImplementedError()


