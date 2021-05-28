"""This class implements the %load magic command"""

help_text = """
The %load magic command has the following syntax:
    > %load [csv file path] [table name] [skip row num](optional)
The %load magic command can load CSV file for updating specific table data.
1. This command does not create a table if the one specified as argument doesn't exist,
   the user needs to create the destination table with the proper schema to match the data in the CSV file.
2. CSV file first line may be header, can set [skip row num] to 1 for skipping.
3. any argument can be enclosed by '' or "", handling string contains spaces.
"""

from mariadb_kernel.maria_magics.line_magic import LineMagic
import pandas
import shlex


class Load(LineMagic):
    def __init__(self, args):
        args_list = shlex.split(args)
        self.csv_file_path = ""
        self.table_name = ""
        self.skip_row_num = 0
        if len(args_list) == 2:
            self.csv_file_path = args_list[0]
            self.table_name = args_list[1]
        elif len(args_list) == 3:
            self.csv_file_path = args_list[0]
            self.table_name = args_list[1]
            self.skip_row_num = args_list[2]

    def name(self):
        return "%load"

    def help(self):
        return help_text

    def execute(self, kernel, data):
        # for error handling
        if self.csv_file_path == "" or self.table_name == "":
            err = "argument num must to be 2 or 3, command need to be %load [csv file path] [table name] [skip row num](optional)"
            kernel._send_message("stderr", err)
            return
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
            display_content = {
                "data": {"text/html": str(kernel.mariadb_client.error_message())},
                "metadata": {},
            }
            kernel.send_response(kernel.iopub_socket, "display_data", display_content)
            return
        result = kernel.mariadb_client.run_statement(
            f"select * from {self.table_name};"
        )
        display_content = {"data": {"text/html": str(result)}, "metadata": {}}
        kernel.send_response(kernel.iopub_socket, "display_data", display_content)

    def generate_value_str(self, value):
        return f"'{value}'"
