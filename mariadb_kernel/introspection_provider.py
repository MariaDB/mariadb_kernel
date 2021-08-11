from typing import Dict, List, Tuple, Union
from bs4.element import Tag

from sqlparse.sql import Function, Identifier, IdentifierList, Parenthesis
from sqlparse.tokens import Keyword, Punctuation, _TokenType
from mariadb_kernel.autocompleter import Autocompleter
from mariadb_kernel.sql_analyze import SQLAnalyze
import re
from prompt_toolkit.document import Document
from mariadb_kernel.completion_engine import suggest_type, last_word
import sqlparse
from bs4 import BeautifulSoup
import json
from pathlib import Path


def first_word(text):
    if not text:  # Empty string
        return ""
    else:
        regex = re.compile(r"^(\w+)")
        matches = regex.search(text)
        if matches:
            return matches.group(0)
        else:
            return ""


class IntrospectionProvider:
    def __init__(self) -> None:
        self.func_doc: dict = {}
        parent_abs_path = str(Path(__file__).parent.resolve())  # ending no /
        with open(parent_abs_path + "/data/func_doc.json", "r") as f:
            self.func_doc = json.load(f)

    def get_instropection(self, document: Document, completer: SQLAnalyze):
        # return word's type and word's text
        last_partial_token_right = first_word(document.text_after_cursor)
        last_partial_token_left = last_word(document.text_before_cursor)
        word = last_partial_token_left + last_partial_token_right

        # get word start's position and end's position
        start_position = document.cursor_position - len(last_partial_token_left)
        end_position = document.cursor_position + len(last_partial_token_right)
        suggestion = suggest_type(document.text, document.text[:start_position])
        suggest_dict = {}
        for suggest in suggestion:
            type = suggest.get("type")
            if type:
                suggest_dict[type] = suggest
        print(f"suggest_dict: {suggest_dict}")
        # Need to check it is being suggested and it exists or not
        # priority: column -> table -> database -> function -> keyword
        if suggest_dict.get("column"):
            # check this is function or not
            parsed_after_cursor = sqlparse.parse(document.text[end_position:])
            if len(parsed_after_cursor) > 0:
                parsed_tokens = parsed_after_cursor[0].tokens
                print(f"parsed_tokens : {parsed_tokens}")
                if len(parsed_tokens) > 0:
                    if suggest_dict.get("function") and isinstance(
                        parsed_tokens[0], Parenthesis
                    ):
                        if word in [
                            function.lower() for function in completer.functions
                        ]:
                            return {"word": word, "type": "function"}

            # if suggest_dict's column has tables not empty, use that as table
            tables = suggest_dict["column"].get("tables")
            curDB: Union[Dict, None] = completer.dbmetadata["tables"].get(
                completer.dbname
            )
            if len(tables) > 0:
                if tables[0][1] != None and curDB:
                    if curDB.get(tables[0][1]) and (
                        word in curDB[tables[0][1]]
                        or f"`{word}`" in curDB[tables[0][1]]
                    ):
                        return {
                            "word": word,
                            "type": "column",
                            "database": completer.dbname,
                            "table": tables[0][1],
                        }
            # search all table for finding which table contains this column
            if curDB:
                for key in curDB.keys():
                    if word in curDB[key] or f"`{word}`" in curDB[key]:
                        # maybe this is wrong
                        return {
                            "word": word,
                            "type": "column",
                            "database": completer.dbname,
                            "table": key,
                        }
            # for statement like 「insert into t1 (a int, b int, c int) VALUES (」 would provide column hint
            parsed_before_cursor = sqlparse.parse(document.text[:end_position])
            if len(parsed_before_cursor) > 0:
                tokens: List[_TokenType] = parsed_before_cursor[0].tokens
                last_match_token_text: str = ""
                hint = ""  # based on the text before 「VALUES」 and after 「insert into」 to get hint
                value_index = 0
                table_name = ""
                print(tokens)
                for token in tokens[::-1]:
                    if token.ttype == Punctuation and str(token) == "(":
                        last_match_token_text = str(token).lower()
                    elif (
                        last_match_token_text == "("
                        and token.ttype == Keyword
                        and str(token).lower() == "values"
                    ):
                        last_match_token_text = str(token).lower()
                    elif (
                        last_match_token_text == "values"
                        and token.ttype == Keyword
                        and str(token).lower() == "into"
                    ):
                        last_match_token_text = str(token).lower()
                        next_token = parsed_before_cursor[0].token_next(
                            parsed_before_cursor[0].token_index(token)
                        )
                        if next_token != (None, None) and next_token and next_token[1]:
                            table_name = next_token[1].get_name()
                    elif (
                        last_match_token_text == "into"
                        and str(token).lower() == "insert"
                    ):
                        return {
                            "type": "column_hint",
                            "hint": hint,
                            "value_index": value_index,
                            "table_name": table_name,
                        }
                    elif last_match_token_text == "values":
                        # accumulate text between 「insert into」 and 「values (」
                        # !!! sqlparse sometimes would resolve Column to Function
                        if isinstance(token, Function):
                            for t in token.tokens:
                                if isinstance(t, Parenthesis):
                                    for in_parenthesis_token in t.tokens:
                                        # (a) => [<Punctuation '(' at 0x7F5AB584EFA0>, <Identifier 'a' at 0x7F5AB5841DD0>, <Punctuation ')' at 0x7F5AB58510A0>]
                                        # (a, b, c) => [<Punctuation '(' at 0x7FE26C6120A0>, <IdentifierList 'a, b, c' at 0x7FE26C603D60>, <Punctuation ')' at 0x7FE26C6123A0>]
                                        if isinstance(
                                            in_parenthesis_token, IdentifierList
                                        ):
                                            cols: List[str] = []
                                            cur_col_text = ""
                                            for curToken in in_parenthesis_token.tokens:
                                                if (
                                                    curToken.ttype == Punctuation
                                                    and str(curToken) in "()"
                                                ):
                                                    continue
                                                if (
                                                    curToken.ttype == Punctuation
                                                    and str(curToken) == ","
                                                ):
                                                    cols.append(cur_col_text.lstrip())
                                                    cur_col_text = ""
                                                else:
                                                    cur_col_text += str(curToken)
                                            cols.append(cur_col_text)
                                            if value_index >= len(cols):
                                                return {
                                                    "type": "column_hint",
                                                    "hint": "out of column",
                                                }
                                            hint = cols[value_index].lstrip()
                                        elif isinstance(
                                            in_parenthesis_token, Identifier
                                        ):
                                            if value_index >= 1:
                                                return {
                                                    "type": "column_hint",
                                                    "hint": "out of column",
                                                }
                    # VALUES (1 ,       => <Keyword 'VALUES' at 0x7F01FBB76FA0>, <Whitespace ' ' at 0x7F01FBB9F040>, <Punctuation '(' at 0x7F01FBB9F0A0>, <Integer '1' at 0x7F01FBB9F100>, <Whitespace ' ' at 0x7F01FBB9F160>, <Punctuation ',' at 0x7F01FBB9F1C0>]
                    # VALUES (1, 2      => <Keyword 'VALUES' at 0x7F01FBB92F40>, <Whitespace ' ' at 0x7F01FBB8DCA0>, <Punctuation '(' at 0x7F01FBB8DD60>, <IdentifierList '1 , 2' at 0x7F01DDDF4660>]
                    # VALUES (1, 2, 3   => <Keyword 'VALUES' at 0x7F01FBB92F40>, <Whitespace ' ' at 0x7F01FBB8DCA0>, <Punctuation '(' at 0x7F01FBB8DD60>, <IdentifierList '1 , 2' at 0x7F01DDDF4660>], <Punctuation ',' at 0x7F3CB98F92E0>]
                    if last_match_token_text == "" and str(token) == ",":
                        value_index += 1
                    if isinstance(token, IdentifierList):
                        for t in token.tokens:
                            if t.ttype == Punctuation and str(t) == ",":
                                value_index += 1
        if suggest_dict.get("table"):
            # If suggest_dict's table has schema field. Regard that as the database, which table belongs
            if suggest_dict["table"].get("schema"):
                table_dbName = suggest_dict["table"].get("schema")
                for t in completer.database_tables:
                    if t[0] == table_dbName and (t[1] == word or t[1] == f"`{word}`"):
                        return {"word": word, "type": "table", "database": table_dbName}
            else:
                table_dbName = completer.dbname
                curDB = completer.dbmetadata["tables"].get(table_dbName)
                if curDB:
                    if curDB.get(word) or curDB.get(f"`{word}`"):
                        return {"word": word, "type": "table", "database": table_dbName}
        if suggest_dict.get("database") and word in completer.databases:
            return {"word": word, "type": "database"}
        if suggest_dict.get("function") and word in [
            function.lower() for function in completer.functions
        ]:
            return {"word": word, "type": "function"}
        if suggest_dict.get("keyword") and word in [
            keyword.lower() for keyword in completer.keywords
        ]:
            return {"word": word, "type": "keyword"}

    def get_left_alignment_table(self, html: str):
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if table and type(table) is Tag:
            table["style"] = "margin-left: 0"
            return str(table)
        else:
            return html

    def render_doc_header(self, name: str):
        return f"<h2 style='color: #0045ad'>{name}</h2>"

    def get_introspection_explain_html(
        self, document: Document, autocompleter: Autocompleter
    ):
        result = self.get_instropection(document, autocompleter.completer)
        if result:
            word_type = result.get("type")
            word = result.get("word")
            if word_type == "keyword":
                # some special keyword would provide more information
                # such as `user` keyword would list all user in database.
                if word:
                    if word == "user":
                        # would show all user list
                        users = autocompleter.executor.users()
                        user_list_text = [user[0] for user in users]
                        users_text = "<br/>".join(user_list_text)
                        return f"{self.render_doc_header('keyword')}<h2>all user list : </h2>{users_text}"
                return self.render_doc_header("keyword")
            elif word_type == "function":
                if word:
                    doc: Union[List[str], None] = self.func_doc.get(
                        word.lower()
                    ) or self.func_doc.get(word.upper())
                    if doc:
                        return f"{self.render_doc_header('function')}{''.join(doc)}"
                return f"{self.render_doc_header('function')}"
            elif word_type == "database":
                if word:
                    tables_html = autocompleter.executor.get_tables_in_db_html(word)
                    return f"{self.render_doc_header('database')}{self.get_left_alignment_table(tables_html)}"
                else:
                    return f"{self.render_doc_header('database')}"
            elif word_type == "table":
                db_name = result.get("database")
                if word and db_name:
                    table_html = autocompleter.executor.get_table_schema_html(
                        word, db_name
                    )
                    limit_num = 5
                    table_rows_html = autocompleter.executor.get_partial_table_row_html(
                        word, db_name, limit_num
                    )
                    return f"""{self.render_doc_header('table')}
                               {self.get_left_alignment_table(table_html)}
                               <b>first {limit_num} row of the table {word}</b><br/>
                               {self.get_left_alignment_table(table_rows_html)}"""
                else:
                    return f"{self.render_doc_header('table')}"
            elif word_type == "column":
                table_name = result.get("table")
                db_name = result.get("database")
                if word and db_name and table_name:
                    column_html = autocompleter.executor.get_column_type_html(
                        word, table_name, db_name
                    )
                    limit_num = 5
                    column_rows_html = autocompleter.executor.get_column_row_html(
                        word, table_name, db_name, limit_num
                    )
                    return f"""{self.render_doc_header('column')}
                               {self.get_left_alignment_table(column_html)}<br/>
                               <b>first {limit_num} row of the column {word}</b><br/>
                               {self.get_left_alignment_table(column_rows_html)}"""
                else:
                    return f"{self.render_doc_header('column')}"
            elif word_type == "column_hint":
                hint = result.get("hint")
                value_index = result.get("value_index")
                table_name = result.get("table_name")
                if (
                    hint != "out of column"
                    and value_index != None
                    and table_name != None
                ):
                    if hint == "":
                        # ex: insert into t1 VALUES (1,2,
                        result = autocompleter.executor.get_column_type_list(
                            table_name, autocompleter.completer.dbname
                        )
                        if int(value_index) >= len(result):
                            hint = "out of column"
                        else:
                            for i, item in enumerate(result):
                                if i == value_index:
                                    hint = item.name + " " + item.type
                    else:
                        result = autocompleter.executor.get_column_type_list(
                            table_name, autocompleter.completer.dbname
                        )
                        for item in result:
                            if item.name == hint:
                                hint = item.name + " " + item.type
                return f"{hint}"


# example
# introspection_provider = IntrospectionProvider()
# completer = SQLAnalyze(Mock())
# result = introspection_provider.get_instropection(
#     Document("select col1 from tbl1", len("select col")), completer
# )
# print(result)
# introspection_provider.get_instropection(
#     Document("select col1 from ", len("select col")), completer
# )
# introspection_provider.get_instropection(
#     Document("insert into db1.tbl1 ", len("insert into db1.t")), completer
# )
# introspection_provider.get_instropection(
#     Document("insert into db2.tbl1 ", len("insert into db2.t")), completer
# )
