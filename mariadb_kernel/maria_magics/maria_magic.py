"""Abstract class that represents a magic command"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.


class MariaMagic:
    def execute(self, args):
        """Executes a magic command.
        **Subclasses must define this method**
        Args: args
        """
        raise NotImplementedError()
