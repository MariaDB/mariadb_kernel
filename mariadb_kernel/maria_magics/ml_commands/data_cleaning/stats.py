# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.maria_magic import MariaMagic
import pandas as pd
import shlex
from distutils import util
import logging
import os
import re

# Optional helper to reliably get current DB name (if available)
try:
    from mariadb_kernel.sql_fetch import SqlFetch
except Exception:
    SqlFetch = None


class Stats(MariaMagic):
    """
    %stats [columns=col1,col2] [include=all|numeric|object] [percentiles=25,50,75] [transpose=true|false]

    Produce a statistical summary of the DataFrame in data["last_select"].

    Examples:
      %stats
        -> numeric summary (count, mean, std, min, 25%, 50%, 75%, max)
      %stats include=all
        -> include all dtypes (object, category, datetime etc.)
      %stats columns=age,salary
        -> summary only for the specified columns
      %stats percentiles=10,90
        -> include the 10th and 90th percentiles (values can be 0-100 or 0-1)
      %stats transpose=true
        -> show summary transposed (rows <-> columns)

    Execution metadata is recorded into table `magic_metadata`.
    """

    def __init__(self, args=""):
        self.args = args

    def type(self):
        return "Line"

    def name(self):
        return "stats"

    def help(self):
        return (
            "%stats [columns=col1,col2] [include=all|numeric|object] "
            "[percentiles=25,50,75] [transpose=true|false]\n"
            "Show statistical summary (uses pandas.DataFrame.describe under the hood)."
            "Execution metadata is recorded in table `magic_metadata`."
        )

    def _str_to_obj(self, s):
        """Cast string tokens to int/float/bool if possible, otherwise return string."""
        try:
            return int(s)
        except ValueError:
            try:
                return float(s)
            except ValueError:
                pass
        try:
            return bool(util.strtobool(s))
        except ValueError:
            return s

    def parse_args(self, input_str):
        """Parse arguments given as key=value pairs (space separated)."""
        if not input_str or input_str.strip() == "":
            return {}
        pairs = dict(token.split("=", 1) for token in shlex.split(input_str))
        for k, v in pairs.items():
            pairs[k] = self._str_to_obj(v)
        return pairs

    def _send_html(self, kernel, df):
        """Send a DataFrame as HTML (fallback to plain text)."""
        try:
            html = df.to_html()
            mime = "text/html"
        except Exception:
            html = str(df)
            mime = "text/plain"
        display_content = {"data": {mime: html}, "metadata": {}}
        kernel.send_response(kernel.iopub_socket, "display_data", display_content)

    def _parse_percentiles(self, pct_arg):
        """
        Accept percentiles as comma-separated list of numbers.
        Values may be 0-100 (e.g. 25) or 0-1 (e.g. 0.25).
        Return list of floats in [0,1] as required by pandas.
        """
        if pct_arg is None:
            return None
        if isinstance(pct_arg, (list, tuple)):
            raw = pct_arg
        else:
            raw = str(pct_arg).split(",")
        out = []
        for item in raw:
            s = str(item).strip()
            if s == "":
                continue
            try:
                v = float(s)
            except ValueError:
                # ignore bad token
                continue
            if v > 1:
                v = v / 100.0
            if 0 <= v <= 1:
                out.append(v)
        # pandas.describe requires percentiles to be sorted and unique
        out = sorted(set(out))
        return out if out else None

    # -------------------- metadata / DB helpers (best-effort) --------------------
    def _get_mariadb_client(self, kernel):
        """Return mariadb_client if present on kernel, else None"""
        return getattr(kernel, "mariadb_client", None)

    def _get_logger(self, kernel):
        """Return a logger on kernel if present, else create a temporary logger"""
        return getattr(kernel, "log", logging.getLogger(__name__))

    def _sql_escape(self, val):
        """Escape a value for SQL single-quoted literal insert. None -> NULL"""
        if val is None:
            return "NULL"
        if not isinstance(val, str):
            val = str(val)
        # double single-quotes for SQL escaping
        return "'" + val.replace("'", "''") + "'"

    def _get_db_name(self, kernel):
        """
        Attempt to determine the currently used DB.
        Prefer SqlFetch if available; otherwise run SELECT DATABASE(); and try to parse.
        Returns empty string if none found.
        """
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

        # Fallback: run SELECT DATABASE();
        if mariadb_client is None:
            return ""
        try:
            result = mariadb_client.run_statement("SELECT DATABASE();")
            if mariadb_client.iserror():
                return ""
            if not result:
                return ""
            # If result is raw HTML table, try to parse with pandas
            try:
                df_list = pd.read_html(result)
                if df_list and isinstance(df_list, list) and len(df_list) > 0:
                    val = df_list[0].iloc[0, 0]
                    if isinstance(val, float) and pd.isna(val):
                        return ""
                    return str(val) if val is not None else ""
            except Exception:
                # if not parseable by pandas, try regex to extract first cell content
                m = re.search(r"<td.*?>(.*?)</td>", str(result), flags=re.S | re.I)
                if m:
                    txt = re.sub(r"<.*?>", "", m.group(1))  # strip tags
                    txt = txt.strip()
                    if txt.lower() == "null" or txt == "":
                        return ""
                    return txt
                # If result is plain text (like the DB name)
                txt = str(result).strip()
                if txt.lower() == "null" or txt == "":
                    return ""
                return txt
        except Exception:
            return ""
        return ""

    def _get_user_name(self, kernel):
        """Try several places to find the current user name; fallback to OS login or empty string."""
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
        """
        Create magic_metadata table if it doesn't exist.
        Columns: id, command_name, arguments, execution_timestamp,
                 affected_columns, operation_status, message, db_name, user_name
        """
        mariadb_client = self._get_mariadb_client(kernel)
        log = self._get_logger(kernel)

        if mariadb_client is None:
            # nothing to do
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
        """
        Insert a metadata row into magic_metadata. Uses NOW() for timestamp.
        """
        mariadb_client = self._get_mariadb_client(kernel)
        log = self._get_logger(kernel)
        if mariadb_client is None:
            return

        table_full_name = f"{db_name}.magic_metadata" if db_name else "magic_metadata"

        # Escape values
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

    # -------------------- end metadata helpers --------------------

    def execute(self, kernel, data):
        """Execute the %stats magic (display-only) and log metadata."""
        df = data.get("last_select")

        # Prepare metadata context early so we can log failures
        db_name = self._get_db_name(kernel)
        user_name = self._get_user_name(kernel)
        try:
            self._ensure_metadata_table(kernel, db_name)
        except Exception:
            try:
                kernel._send_message("stdout", "Warning: failed to ensure metadata table (continuing).")
            except Exception:
                pass

        if df is None:
            msg = "No last_select found in kernel data."
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

        if hasattr(df, "empty") and df.empty:
            msg = "There is no data to summarize (empty DataFrame)."
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

        # columns handling
        cols_arg = args.get("columns", None)
        if isinstance(cols_arg, str):
            columns = [c.strip() for c in cols_arg.split(",") if c.strip()]
        elif isinstance(cols_arg, (list, tuple)):
            columns = list(cols_arg)
        else:
            columns = None

        # include: pandas.describe 'include' parameter (None default -> numeric)
        include = args.get("include", "numeric")
        if include not in ("numeric", "object", "all"):
            # allow user to pass pandas dtypes-like include, but restrict to these for simplicity
            include = "numeric"
        include_param = None
        if include == "all":
            include_param = "all"
        elif include == "object":
            include_param = object
        else:
            include_param = None  # pandas default -> numeric only

        # percentiles
        percentiles_arg = args.get("percentiles", None)
        percentiles = self._parse_percentiles(percentiles_arg)

        transpose = bool(args.get("transpose", False))

        # subset dataframe if columns specified
        try:
            subdf = df[columns] if columns is not None else df
        except KeyError as e:
            msg = f"Column not found: {e}"
            kernel._send_message("stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns="\n".join(columns) if columns else "",
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return

        # call pandas describe
        try:
            describe_kwargs = {}
            if percentiles is not None:
                describe_kwargs["percentiles"] = percentiles
            if include_param is not None:
                describe_kwargs["include"] = include_param

            result = subdf.describe(**describe_kwargs)
            # For object dtypes, pandas describe may include top/freq; that's fine.
            if transpose:
                try:
                    result = result.transpose()
                except Exception:
                    # fallback without transposing if something goes wrong
                    pass

            self._send_html(kernel, result)

            # Insert metadata (success)
            affected_columns_str = "\n".join(columns) if columns else "ALL_COLUMNS"
            pct_str = ",".join(str(p) for p in (percentiles or [])) if percentiles else ""
            message = f"Stats computed for {len(result.columns) if hasattr(result, 'columns') else 'N'} column(s); total_rows={len(subdf)}; percentiles={pct_str}; include={include}."
            operation_status = "success"
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns=affected_columns_str,
                    operation_status=operation_status,
                    message=message,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                # do not interrupt flow if logging fails
                pass

        except Exception as e:
            msg = f"Error computing statistics: {e}"
            kernel._send_message("stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns="\n".join(columns) if columns else "ALL_COLUMNS",
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
