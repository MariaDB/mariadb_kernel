"""This class implements the %load magic commdnad"""

help_text = """
The %laod magic command has the following syntax:
    > %loav [csv file path] [be updated table name]
The %load magic command can load CSV file for updating specific table data 
"""

from mariadb_kernel.maria_magics.line_magic import LineMagic
import pandas

# The class inherits LineMagic because it is a line magic command
# Cell magic commands have to inherit the CellMagic class
class Load(LineMagic):
    def __init__(self, args):
        temp_args = args.split(" ")
        self.csv_file_path = ""
        self.table_name = ""
        if len(temp_args) == 2:
            self.csv_file_path = args.split(" ")[0]
            self.table_name = args.split(" ")[1]

    def name(self):
        return "%load"

    def help(self):
        return help_text

    def execute(self, kernel, data):
        # for error handling
        if self.csv_file_path == "" or self.table_name == "":
            err = "argument num must to be 2"
            kernel._send_message("stderr", err)
            return
        try:
            open(self.csv_file_path)
        except FileNotFoundError:
            err = "CSV file not found"
            kernel._send_message("stderr", err)
            return
        # kernel.log.info(json.dumps(self.args))
        data_frame = pandas.read_csv(self.csv_file_path)
        cols_info = "(" + ",".join(str(v) for v in data_frame.columns) + ")"
        rows = [
            "(" + " ,".join(self.generate_value_str(v) for v in values) + ")"
            for index, values in data_frame.iterrows()
        ]
        rows_info = ",".join(rows)
        insert_sql = f"INSERT INTO {self.table_name} {cols_info} VALUES {rows_info};"
        kernel.log.info(insert_sql)
        insert_result: str = kernel.mariadb_client.run_statement(insert_sql)
        # ERROR 1064 (42000): You have an error in your SQL syntax
        if insert_result.startswith("ERROR 1064"):
            err = "maybe CSV file not match the table schema, then can't insert into table"
            kernel._send_message("stderr", err)
            return
        result = kernel.mariadb_client.run_statement(
            f"select * from {self.table_name};"
        )
        display_content = {"data": {"text/html": str(result)}, "metadata": {}}
        kernel.send_response(kernel.iopub_socket, "display_data", display_content)

    def generate_value_str(self, value):
        return f"'{value}'"
