# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.maria_magic import MariaMagic
import shlex
from distutils import util
import pandas as pd
from sklearn.model_selection import train_test_split
import logging
import os
import re

# optional helper to reliably get current DB name (if available)
try:
    from mariadb_kernel.sql_fetch import SqlFetch
except Exception:
    SqlFetch = None


class SplitData(MariaMagic):
    """
    %splitdata [test_size=0.2] [val_size=0.1] [stratify=colname] [shuffle=True|False]
               [random_state=42] [inplace=True|False] [train_name=last_select_train]
               [test_name=last_select_test] [val_name=last_select_val]

    Split the current data["last_select"] DataFrame into train/test/(validation).

    Execution metadata is recorded into table `magic_metadata` with fields:
      id, command_name, arguments, execution_timestamp, affected_columns,
      operation_status, message, db_name, user_name
    """

    def __init__(self, args=""):
        self.args = args

    def type(self):
        return "Line"

    def name(self):
        return "splitdata"

    def help(self):
        return (
            "%splitdata [test_size=0.2] [val_size=0.1] [stratify=colname] [shuffle=True|False]\n"
            "[random_state=42] [inplace=True|False] [train_name=name] [test_name=name] [val_name=name]\n"
            "Split last_select into train/test/(val). Execution metadata recorded in magic_metadata."
        )

    def _str_to_obj(self, s):
        try:
            return int(s)
        except ValueError:
            try:
                return float(s)
            except ValueError:
                pass
        try:
            return bool(util.strtobool(s))
        except Exception:
            if isinstance(s, str) and len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
                return s[1:-1]
            return s

    def parse_args(self, input_str):
        if not input_str or input_str.strip() == "":
            return {}
        pairs = dict(token.split("=", 1) for token in shlex.split(input_str))
        for k, v in pairs.items():
            pairs[k] = self._str_to_obj(v)
        return pairs

    def _send_html(self, kernel, df, title=None):
        try:
            html = df.to_html(index=False)
            if title:
                html = f"<h4>{title}</h4>" + html
            kernel.send_response(kernel.iopub_socket, "display_data",
                                 {"data": {"text/html": html}, "metadata": {}})
        except Exception:
            pass

    # --------------- metadata / DB helpers (best-effort) ----------------
    def _get_mariadb_client(self, kernel):
        return getattr(kernel, "mariadb_client", None)

    def _get_logger(self, kernel):
        return getattr(kernel, "log", logging.getLogger(__name__))

    def _sql_escape(self, val):
        if val is None:
            return "NULL"
        if not isinstance(val, str):
            val = str(val)
        return "'" + val.replace("'", "''") + "'"

    def _get_db_name(self, kernel):
        mariadb_client = self._get_mariadb_client(kernel)
        log = self._get_logger(kernel)

        # Try SqlFetch if available
        if SqlFetch is not None and mariadb_client is not None:
            try:
                sf = SqlFetch(mariadb_client, log)
                dbname = sf.get_db_name()
                if isinstance(dbname, str):
                    return dbname
            except Exception:
                log.debug("SqlFetch available but .get_db_name() failed; falling back.")

        if mariadb_client is None:
            return ""

        try:
            result = mariadb_client.run_statement("SELECT DATABASE();")
            if mariadb_client.iserror() or not result:
                return ""
            try:
                dfs = pd.read_html(result)
                if dfs and len(dfs) > 0:
                    val = dfs[0].iloc[0, 0]
                    if isinstance(val, float) and pd.isna(val):
                        return ""
                    return str(val) if val is not None else ""
            except Exception:
                m = re.search(r"<td.*?>(.*?)</td>", str(result), flags=re.S | re.I)
                if m:
                    txt = re.sub(r"<.*?>", "", m.group(1)).strip()
                    if txt.lower() == "null" or txt == "":
                        return ""
                    return txt
                txt = str(result).strip()
                if txt.lower() == "null" or txt == "":
                    return ""
                return txt
        except Exception:
            return ""
        return ""

    def _get_user_name(self, kernel):
        candidates = [
            getattr(kernel, "user_name", None),
            getattr(kernel, "username", None),
            getattr(kernel, "user", None),
            getattr(kernel, "session", None),
        ]
        for cand in candidates:
            if cand is None:
                continue
            if isinstance(cand, str) and cand.strip():
                return cand
            try:
                maybe = getattr(cand, "user", None)
                if isinstance(maybe, str) and maybe.strip():
                    return maybe
            except Exception:
                pass
        try:
            return os.getlogin()
        except Exception:
            return ""

    def _ensure_metadata_table(self, kernel, db_name):
        mariadb_client = self._get_mariadb_client(kernel)
        log = self._get_logger(kernel)
        if mariadb_client is None:
            return
        table_full_name = f"{db_name}.magic_metadata" if db_name else "magic_metadata"
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_full_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            command_name VARCHAR(255),
            arguments TEXT,
            execution_timestamp DATETIME,
            affected_columns TEXT,
            operation_status VARCHAR(50),
            message TEXT,
            db_name VARCHAR(255),
            user_name VARCHAR(255),
            rollback_token VARCHAR(255),
            backup_table VARCHAR(255),
            original_table VARCHAR(255)
        );
        """
        try:
            mariadb_client.run_statement(create_sql)
            if mariadb_client.iserror():
                log.error("Error creating magic_metadata table.")
        except Exception as e:
            log.error(f"Failed to ensure magic_metadata table: {e}")

    def _insert_metadata(self, kernel, command_name, arguments, affected_columns,
                         operation_status, message, db_name, user_name):
        mariadb_client = self._get_mariadb_client(kernel)
        log = self._get_logger(kernel)
        if mariadb_client is None:
            return
        table_full_name = f"{db_name}.magic_metadata" if db_name else "magic_metadata"

        args_sql = self._sql_escape(arguments)
        affected_sql = self._sql_escape(affected_columns)
        status_sql = self._sql_escape(operation_status)
        message_sql = self._sql_escape(message)
        db_sql = self._sql_escape(db_name)
        user_sql = self._sql_escape(user_name)

        insert_sql = f"""
        INSERT INTO {table_full_name}
            (command_name, arguments, execution_timestamp, affected_columns,
             operation_status, message, db_name, user_name)
        VALUES (
            {self._sql_escape(command_name)},
            {args_sql},
            NOW(),
            {affected_sql},
            {status_sql},
            {message_sql},
            {db_sql},
            {user_sql}
        );
        """
        try:
            mariadb_client.run_statement(insert_sql)
            if mariadb_client.iserror():
                log.error("Error inserting into magic_metadata.")
        except Exception as e:
            log.error(f"Exception while inserting metadata: {e}")
    # ---------------- end metadata helpers ----------------

    def execute(self, kernel, data):
        df = data.get("last_select")

        # prepare metadata context
        db_name = self._get_db_name(kernel)
        user_name = self._get_user_name(kernel)
        try:
            self._ensure_metadata_table(kernel, db_name)
        except Exception:
            try:
                kernel._send_message("stdout", "Warning: failed to ensure metadata table (continuing).")
            except Exception:
                pass

        if df is None or df.empty:
            msg = "No last_select found or DataFrame is empty."
            kernel._send_message("stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns="",
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return

        try:
            args = self.parse_args(self.args)
        except Exception:
            msg = "Error parsing arguments. Use key=value syntax."
            kernel._send_message("stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns="",
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return

        # Defaults
        test_size_arg = args.get("test_size", 0.2)
        val_size_arg = args.get("val_size", 0.0)
        stratify_col = args.get("stratify", None)
        shuffle = bool(args.get("shuffle", True))
        random_state = args.get("random_state", None)
        inplace = bool(args.get("inplace", True))

        train_name = args.get("train_name", "last_select_train")
        test_name = args.get("test_name", "last_select_test")
        val_name = args.get("val_name", "last_select_val")

        # Validate dataset
        n_total = len(df)
        if n_total == 0:
            msg = "DataFrame has no rows to split."
            kernel._send_message("stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns="",
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return

        # Helper to interpret sizes (int count or fraction)
        def interpret_size(size_arg, total):
            if isinstance(size_arg, int):
                if size_arg < 0:
                    raise ValueError("Sizes must be non-negative.")
                return float(size_arg) / total
            try:
                size_f = float(size_arg)
            except Exception:
                raise ValueError("Size must be an int or float.")
            if size_f < 0:
                raise ValueError("Sizes must be non-negative.")
            if 0 <= size_f < 1:
                return size_f
            # If provided >=1 and integer-like, treat as count
            if size_f >= 1 and abs(size_f - int(size_f)) < 1e-9:
                if int(size_f) > total:
                    raise ValueError("Size count larger than dataset.")
                return float(int(size_f)) / total
            # fractions >= 1 are invalid
            raise ValueError("If numeric and >=1, size must be an integer count <= total rows.")

        try:
            test_frac = interpret_size(test_size_arg, n_total)
            val_frac = interpret_size(val_size_arg, n_total)
        except ValueError as e:
            msg = f"Error interpreting sizes: {e}"
            kernel._send_message("stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns="",
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return

        if test_frac + val_frac >= 1.0:
            msg = "Sum of test_size and val_size must be less than 1.0."
            kernel._send_message("stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns="",
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return

        # Prepare stratify arrays if requested
        stratify_arr = None
        if stratify_col:
            if stratify_col not in df.columns:
                msg = f"Stratify column '{stratify_col}' not found in DataFrame."
                kernel._send_message("stderr", msg)
                try:
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns="",
                        operation_status="error",
                        message=msg,
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    pass
                return
            stratify_arr = df[stratify_col].values

        # Run splits
        try:
            # First split off the test set (test_frac of original)
            if test_frac > 0:
                train_val_df, test_df = train_test_split(
                    df,
                    test_size=test_frac,
                    shuffle=shuffle,
                    random_state=random_state,
                    stratify=stratify_arr if stratify_arr is not None else None
                )
            else:
                train_val_df = df.copy(deep=True)
                test_df = pd.DataFrame(columns=df.columns)

            # If no val requested, train = train_val_df
            if val_frac <= 0:
                train_df = train_val_df
                val_df = pd.DataFrame(columns=df.columns)
            else:
                rel_val_frac = val_frac / (1.0 - test_frac)
                stratify_arr_second = None
                if stratify_arr is not None:
                    stratify_arr_second = train_val_df[stratify_col].values
                train_df, val_df = train_test_split(
                    train_val_df,
                    test_size=rel_val_frac,
                    shuffle=shuffle,
                    random_state=random_state,
                    stratify=stratify_arr_second if stratify_arr_second is not None else None
                )

            # Store results in data dict under requested names
            data[test_name] = test_df
            data[val_name] = val_df
            data[train_name] = train_df

            if inplace:
                data["last_select"] = train_df

            # Report sizes
            msg = (
                f"Split completed: total={n_total}, train={len(train_df)}, "
                f"test={len(test_df)}, val={len(val_df)}."
            )
            kernel._send_message("stdout", msg)

            # Display small previews
            try:
                if not train_df.empty:
                    self._send_html(kernel, train_df.head(20), title=f"Train ({len(train_df)} rows)")
                if not val_df.empty:
                    self._send_html(kernel, val_df.head(20), title=f"Validation ({len(val_df)} rows)")
                if not test_df.empty:
                    self._send_html(kernel, test_df.head(20), title=f"Test ({len(test_df)} rows)")
            except Exception:
                pass

            # Insert metadata (success)
            try:
                args_for_db = self.args if isinstance(self.args, str) else str(self.args)
                affected_columns = stratify_col if stratify_col else "ALL_COLUMNS"
                message = (
                    f"train_name={train_name}, test_name={test_name}, val_name={val_name}\n"
                    f"train_count={len(train_df)}, test_count={len(test_df)}, val_count={len(val_df)}\n"
                    f"test_frac={test_frac}, val_frac={val_frac}, shuffle={shuffle}, random_state={random_state}"
                )
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=args_for_db,
                    affected_columns=affected_columns,
                    operation_status="success",
                    message=message,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                try:
                    kernel._send_message("stdout", "Warning: failed to write metadata (continuing).")
                except Exception:
                    pass

        except Exception as e:
            msg = f"Error during splitting: {e}"
            kernel._send_message("stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns=stratify_col if stratify_col else "",
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return
