"""This class implements the %load magic command"""

help_text = """
The %load magic command has the following syntax:
    > %load csv_file_path table_name [skip_row_num]
The %load magic command can load CSV file for updating specific table data.

This command does not create a table if the one specified as argument doesn't exist,
the user needs to create the destination table with the proper schema to match the data in the CSV file.

CSV file first line may be header, can set [skip row num] to 1 for skipping header.

Any argument can be enclosed by ' ' or " ", handling cases that argument contains spaces.
"""

from mariadb_kernel.maria_magics.line_magic import LineMagic
import shlex


class Load(LineMagic):
    def __init__(self, args):
        self.args_list = shlex.split(args)

    def name(self):
        return "%load"

    def help(self):
        return help_text

    def execute(self, kernel, data):
        self.skip_row_num = 0

        if len(self.args_list) < 2:
            kernel._send_message(
                "stderr",
                "There was an error while parsing the arguments.\n"
                + "Please check %lsmagic on how to use the magic command",
            )
            return
        else:
            self.csv_file_path = self.args_list[0]
            self.table_name = self.args_list[1]
            if len(self.args_list) > 2:
                self.skip_row_num = int(self.args_list[3])

        try:
            open(self.csv_file_path).close()
        except FileNotFoundError:
            err = "CSV file not found"
            kernel._send_message("stderr", err)
            return

        use_csv_update_table_cmd = f"""LOAD DATA LOCAL INFILE '{self.csv_file_path}'
                       IGNORE
                       INTO TABLE {self.table_name}
                       FIELDS TERMINATED BY ','
                       IGNORE {self.skip_row_num} LINES
                       ;"""
        kernel.mariadb_client.run_statement(use_csv_update_table_cmd)
        if kernel.mariadb_client.iserror():
            kernel._send_message("stderr", kernel.mariadb_client.error_message())
            return
        result = kernel.mariadb_client.run_statement(
            f"select * from {self.table_name} limit 5;"
        )
        display_content = {
            "data": {"text/html": str(result + f"<b>...only show 5 rows<b/>")},
            "metadata": {},
        }
        kernel.send_response(kernel.iopub_socket, "display_data", display_content)
