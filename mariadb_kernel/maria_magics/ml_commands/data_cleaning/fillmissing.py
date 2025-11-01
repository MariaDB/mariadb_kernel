# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.maria_magic import MariaMagic
import shlex
from distutils import util
import pandas as pd
import logging
import os
import re
import uuid
import time

# Optional helper to reliably get current DB name (if available)
try:
    from mariadb_kernel.sql_fetch import SqlFetch
except Exception:
    SqlFetch = None


class FillMissing(MariaMagic):
    """
    %fillmissing [columns=col1,col2,...] [strategy=mean|median|mode|constant]
                 [value=const] [mode=preview|apply|rollback] [table=schema.table]
                 [confirm=true|false] [sample_size=100]

    Behavior:
      - preview: shows what would be filled (counts, sample rows with nulls, and computed fill values)
      - apply: performs the fill (locally or on DB if table= specified)
      - rollback: attempts to restore a backup created by an apply (requires mariadb_client + rollback_token or will use latest by user)

    Notes:
      - DB apply uses a CTAS + atomic RENAME pattern so the original is preserved as <table>_backup_<token>.
      - For DB fill values we compute values using SQL when possible (AVG for mean, GROUP BY+COUNT for mode).
        Median uses a sampling fallback to compute the median in Python (best-effort).
      - Execution metadata is recorded in table `magic_metadata` (including rollback_token, backup_table, original_table).
    """

    def __init__(self, args=""):
        self.args = args

    def type(self):
        return "Line"

    def name(self):
        return "fillmissing"

    def help(self):
        return (
            "%fillmissing [columns=col1,col2,...] [strategy=mean|median|mode|constant] [value=const]\n"
            "          [mode=preview|apply|rollback] [table=schema.table] [confirm=true|false] [sample_size=100]\n"
            "Fills missing values in data['last_select'] or in DB table when table= is provided."
        )

    def _str_to_obj(self, s):
        """Cast simple strings to Python objects where sensible."""
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
            # Remove surrounding quotes if present so value="abc" becomes abc (still as string)
            if isinstance(s, str) and len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
                return s[1:-1]
            return s

    def parse_args(self, input_str):
        """Parse key=value arguments (keeps behavior consistent with other magics)."""
        if not input_str or input_str.strip() == "":
            return {}
        pairs = dict(token.split("=", 1) for token in shlex.split(input_str))
        for k, v in pairs.items():
            pairs[k] = self._str_to_obj(v)
        return pairs

    def _send_html(self, kernel, df):
        """Display DataFrame as HTML (fallback to text if needed)."""
        try:
            html_repr = df.to_html(index=False)
            mime = "text/html"
        except Exception:
            html_repr = str(df)
            mime = "text/plain"
        display_content = {"data": {mime: html_repr}, "metadata": {}}
        kernel.send_response(kernel.iopub_socket, "display_data", display_content)

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
        Columns include fields to support rollback tracking.
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
                         operation_status, message, db_name, user_name,
                         rollback_token=None, backup_table=None, original_table=None):
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
        rollback_sql = self._sql_escape(rollback_token)
        backup_sql = self._sql_escape(backup_table)
        original_sql = self._sql_escape(original_table)

        insert_sql = f"""
        INSERT INTO {table_full_name}
            (command_name, arguments, execution_timestamp, affected_columns,
             operation_status, message, db_name, user_name, rollback_token, backup_table, original_table)
        VALUES (
            {self._sql_escape(command_name)},
            {args_sql},
            NOW(),
            {affected_sql},
            {status_sql},
            {message_sql},
            {db_sql},
            {user_sql},
            {rollback_sql},
            {backup_sql},
            {original_sql}
        );
        """
        try:
            mariadb_client.run_statement(insert_sql)
            # swallow errors but log
            if mariadb_client.iserror():
                log.error("Error inserting into magic_metadata.")
        except Exception as e:
            log.error(f"Exception while inserting metadata: {e}")

    def _acquire_lock(self, mariadb_client, lock_name, timeout=10):
        try:
            mariadb_client.run_statement(f"SELECT GET_LOCK('{lock_name}', {int(timeout)});")
            if mariadb_client.iserror():
                return False
            return True
        except Exception:
            return False

    def _release_lock(self, mariadb_client, lock_name):
        try:
            mariadb_client.run_statement(f"SELECT RELEASE_LOCK('{lock_name}');")
        except Exception:
            pass

    def _table_exists(self, mariadb_client, table_full_name):
        try:
            mariadb_client.run_statement(f"SELECT 1 FROM {table_full_name} LIMIT 1;")
            return not mariadb_client.iserror()
        except Exception:
            return False

    # -------------------- end metadata helpers --------------------

    def _compute_fill_value_db(self, mariadb_client, table_full, col, strategy, const_value, sample_size=100):
        """
        Compute fill value for a DB column using SQL when possible; otherwise fall back to sampling.
        Returns (success_bool, fill_value or None, message)
        """
        try:
            # constant
            if strategy == "constant":
                return True, const_value, "constant provided"

            # mean -> AVG
            if strategy == "mean":
                try:
                    out = mariadb_client.run_statement(f"SELECT AVG({col}) FROM {table_full} WHERE {col} IS NOT NULL;")
                    if mariadb_client.iserror() or not out:
                        return False, None, "AVG query failed"
                    # parse result
                    try:
                        df_list = pd.read_html(out)
                        if df_list and len(df_list) > 0:
                            val = df_list[0].iloc[0, 0]
                            # convert to numeric if possible
                            try:
                                valf = float(val)
                                return True, valf, "mean via SQL"
                            except Exception:
                                return True, val, "mean via SQL (non-numeric parse)"
                    except Exception:
                        # regex fallback
                        m = re.search(r"<td.*?>(.*?)</td>", str(out), flags=re.S | re.I)
                        if m:
                            txt = re.sub(r"<.*?>", "", m.group(1)).strip()
                            try:
                                return True, float(txt), "mean via SQL (regex)"
                            except Exception:
                                return True, txt, "mean via SQL (regex)"
                    return False, None, "Could not parse AVG result"
                except Exception:
                    return False, None, "AVG query exception"

            # mode -> most frequent value via GROUP BY
            if strategy == "mode":
                try:
                    out = mariadb_client.run_statement(f"SELECT {col}, COUNT(*) AS cnt FROM {table_full} WHERE {col} IS NOT NULL GROUP BY {col} ORDER BY cnt DESC LIMIT 1;")
                    if mariadb_client.iserror() or not out:
                        return False, None, "MODE query failed"
                    try:
                        df_list = pd.read_html(out)
                        if df_list and len(df_list) > 0:
                            val = df_list[0].iloc[0, 0]
                            return True, val, "mode via SQL"
                    except Exception:
                        m = re.search(r"<td.*?>(.*?)</td>", str(out), flags=re.S | re.I)
                        if m:
                            txt = re.sub(r"<.*?>", "", m.group(1)).strip()
                            return True, txt, "mode via SQL (regex)"
                    return False, None, "Could not parse mode result"
                except Exception:
                    return False, None, "MODE query exception"

            # median -> sampling fallback: select a sample of non-null values and compute median in pandas
            if strategy == "median":
                try:
                    out = mariadb_client.run_statement(f"SELECT {col} FROM {table_full} WHERE {col} IS NOT NULL LIMIT {int(sample_size)};")
                    if mariadb_client.iserror() or not out:
                        return False, None, "Median: sample query failed"
                    try:
                        df_list = pd.read_html(out)
                        if df_list and len(df_list) > 0:
                            series = df_list[0].iloc[:, 0]
                            # convert to numeric where possible
                            try:
                                series_num = pd.to_numeric(series, errors="coerce").dropna()
                                if series_num.empty:
                                    return False, None, "Median: non-numeric or all missing in sample"
                                med = series_num.median()
                                return True, float(med), "median via sampling"
                            except Exception:
                                return False, None, "Median: numeric conversion failed"
                    except Exception:
                        return False, None, "Median: parsing sample failed"
                except Exception:
                    return False, None, "Median: sample query exception"

            return False, None, "Unknown strategy"
        except Exception as e:
            return False, None, f"Exception computing fill value: {e}"

    def execute(self, kernel, data):
        """Execute the fillmissing magic (preview/apply/rollback) and log metadata."""
        df = data.get("last_select")
        if df is None:
            kernel._send_message("stderr", "No last_select found in kernel data.")
            return

        if hasattr(df, "empty") and df.empty:
            kernel._send_message("stderr", "There is no data to process (empty DataFrame).")
            return

        try:
            args = self.parse_args(self.args)
        except Exception:
            kernel._send_message("stderr", "Error parsing arguments. Use key=value syntax.")
            return

        # parse columns argument
        columns_arg = args.get("columns", None)
        if isinstance(columns_arg, str):
            target_columns = [c.strip() for c in columns_arg.split(",") if c.strip()]
        elif isinstance(columns_arg, (list, tuple)):
            target_columns = list(columns_arg)
        else:
            target_columns = None

        # mode: preview|apply|rollback
        mode = str(args.get("mode", "preview")).lower()
        mode = mode if mode in {"preview", "apply", "rollback"} else "preview"

        # other args
        strategy = args.get("strategy", "mean")
        if isinstance(strategy, str):
            strategy = strategy.lower()
        else:
            strategy = str(strategy).lower()

        allowed = {"mean", "median", "mode", "constant"}
        if strategy not in allowed:
            kernel._send_message("stderr", f"Unknown strategy '{strategy}'. Allowed: {', '.join(allowed)}")
            return

        value_provided = "value" in args
        const_value = args.get("value", None)

        if strategy == "constant" and not value_provided and mode != "preview":
            kernel._send_message("stderr", "Strategy 'constant' requires a 'value=...' argument.")
            return

        table_full = args.get("table", None)
        confirm = bool(args.get("confirm", False))
        sample_size = int(args.get("sample_size", 100))

        mariadb_client = self._get_mariadb_client(kernel)
        log = self._get_logger(kernel)
        db_name = self._get_db_name(kernel)
        user_name = self._get_user_name(kernel)

        # determine local target columns if not provided
        if target_columns is None:
            target_columns = list(df.columns)
        else:
            missing_cols = [c for c in target_columns if c not in df.columns]
            if missing_cols:
                kernel._send_message("stderr", f"Column(s) not found in last_select: {', '.join(missing_cols)}")
                # log metadata for failure
                try:
                    self._ensure_metadata_table(kernel, db_name)
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns="\n".join(target_columns),
                        operation_status="error",
                        message=f"Column(s) not found: {', '.join(missing_cols)}",
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    pass
                return

        # Ensure metadata table exists (best-effort)
        try:
            self._ensure_metadata_table(kernel, db_name)
        except Exception:
            try:
                kernel._send_message("stdout", "Warning: failed to ensure metadata table (continuing).")
            except Exception:
                pass

        # --- PREVIEW MODE -------------------------------------------------
        if mode == "preview":
            try:
                messages = []
                # missing count per column in the DataFrame
                missing_counts = {col: int(df[col].isnull().sum()) for col in target_columns}
                summary_lines = [f"{col}: missing={count}" for col, count in missing_counts.items()]
                kernel._send_message("stdout", "PREVIEW: missing counts per column:\n" + "\n".join(summary_lines))

                # compute would-be fill values (local logic); for DB-target, attempt to compute via DB if mariadb_client available
                computed = {}
                for col in target_columns:
                    # local compute based on df contents
                    if strategy == "constant":
                        computed[col] = (True, const_value, "constant provided")
                    else:
                        # compute on the sample df (non-null values)
                        series = df[col].dropna()
                        if series.empty:
                            computed[col] = (False, None, "no non-missing values in preview sample")
                        else:
                            if strategy == "mean":
                                if pd.api.types.is_numeric_dtype(series):
                                    computed[col] = (True, float(series.mean()), "mean via local preview")
                                else:
                                    computed[col] = (False, None, "not numeric; cannot compute mean locally")
                            elif strategy == "median":
                                if pd.api.types.is_numeric_dtype(series):
                                    computed[col] = (True, float(series.median()), "median via local preview")
                                else:
                                    computed[col] = (False, None, "not numeric; cannot compute median locally")
                            elif strategy == "mode":
                                modes = series.mode(dropna=True)
                                if not modes.empty:
                                    computed[col] = (True, modes.iloc[0], "mode via local preview")
                                else:
                                    computed[col] = (False, None, "no mode found in local preview")

                    # if DB target specified and mariadb_client available, try DB-based compute (overrides local)
                    if table_full and mariadb_client is not None:
                        ok, val, msg = self._compute_fill_value_db(mariadb_client, table_full, col, strategy, const_value, sample_size=sample_size)
                        computed[col] = (ok, val, f"db:{msg}" if msg else "db:unknown")

                # display computed fill values
                comp_lines = []
                for col, (ok, val, msg) in computed.items():
                    if ok:
                        comp_lines.append(f"{col}: would fill with -> {val} ({msg})")
                    else:
                        comp_lines.append(f"{col}: could NOT determine fill value ({msg}); would skip")
                kernel._send_message("stdout", "PREVIEW: computed fill-values (best-effort):\n" + "\n".join(comp_lines))

                # show a sample of rows that would be affected (rows with any NULL in target_columns)
                mask = df[target_columns].isnull().any(axis=1)
                sample_rows = df[mask].head(sample_size)
                if not sample_rows.empty:
                    # Add a helper column to indicate which columns are null in that row
                    def nulls_in_row(r):
                        return ",".join([c for c in target_columns if pd.isnull(r.get(c))])
                    sample_preview = sample_rows.copy()
                    sample_preview["_null_columns"] = sample_preview.apply(nulls_in_row, axis=1)

                    # --- NEW: compute filled-preview columns so the user can see what values would be used ---
                    for c in target_columns:
                        filled_col = f"{c}_filled_preview"
                        ok, fill_val, _ = computed.get(c, (False, None, ""))
                        try:
                            if ok and fill_val is not None:
                                # use pandas fillna on the preview sample to show the to-be-filled value
                                sample_preview[filled_col] = sample_preview[c].fillna(fill_val)
                            else:
                                # no computed fill value: show original values so preview is still informative
                                sample_preview[filled_col] = sample_preview[c]
                        except Exception:
                            # fallback elementwise: preserve original when something goes wrong
                            def _fill_elem(v):
                                try:
                                    if pd.isna(v) and ok and fill_val is not None:
                                        return fill_val
                                    return v
                                except Exception:
                                    return v
                            sample_preview[filled_col] = sample_preview[c].apply(_fill_elem)

                    try:
                        self._send_html(kernel, sample_preview)
                    except Exception:
                        kernel._send_message("stdout", str(sample_preview.head()))
                else:
                    kernel._send_message("stdout", "PREVIEW: no rows with missing values in the preview sample.")

                # log preview metadata
                try:
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns='\n'.join(target_columns) if target_columns else 'ALL_COLUMNS',
                        operation_status='preview',
                        message='preview_computed_fill_values',
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    pass

            except Exception as e:
                kernel._send_message("stderr", f"Error during preview: {e}")
            return

        # --- ROLLBACK MODE ---------------------------------------------
        if mode == "rollback":
            if mariadb_client is None:
                kernel._send_message("stderr", "Rollback requested but no mariadb_client available.")
                return
            token = args.get("rollback_token", None)
            try:
                if not token:
                    # find latest metadata for this command and user
                    mariadb_client.run_statement(f"SELECT rollback_token, backup_table, original_table FROM {db_name}.magic_metadata WHERE command_name={self._sql_escape(self.name())} AND user_name={self._sql_escape(user_name)} ORDER BY execution_timestamp DESC LIMIT 1;")
                    if mariadb_client.iserror():
                        kernel._send_message("stderr", "Could not find metadata for rollback (check permissions).")
                        return
                    out = mariadb_client.run_statement(f"SELECT rollback_token FROM {db_name}.magic_metadata WHERE command_name={self._sql_escape(self.name())} AND user_name={self._sql_escape(user_name)} ORDER BY execution_timestamp DESC LIMIT 1;")
                    m = re.search(r"<td.*?>(.*?)</td>", str(out), flags=re.S | re.I)
                    if m:
                        token = re.sub(r"<.*?>", "", m.group(1)).strip()
                if not token:
                    kernel._send_message("stderr", "No rollback_token found; cannot rollback safely.")
                    return

                # fetch backup_table and original_table
                out = mariadb_client.run_statement(f"SELECT backup_table, original_table FROM {db_name}.magic_metadata WHERE rollback_token={self._sql_escape(token)} LIMIT 1;")
                m = re.search(r"<td.*?>(.*?)</td>.*?<td.*?>(.*?)</td>", str(out), flags=re.S | re.I)
                backup_table = None
                original_table = None
                if m:
                    backup_table = re.sub(r"<.*?>", "", m.group(1)).strip()
                    original_table = re.sub(r"<.*?>", "", m.group(2)).strip()
                else:
                    # fallback single column parses
                    try:
                        out_b = mariadb_client.run_statement(f"SELECT backup_table FROM {db_name}.magic_metadata WHERE rollback_token={self._sql_escape(token)} LIMIT 1;")
                        mb = re.search(r"<td.*?>(.*?)</td>", str(out_b), flags=re.S | re.I)
                        if mb:
                            backup_table = re.sub(r"<.*?>", "", mb.group(1)).strip()
                    except Exception:
                        pass
                    try:
                        out_o = mariadb_client.run_statement(f"SELECT original_table FROM {db_name}.magic_metadata WHERE rollback_token={self._sql_escape(token)} LIMIT 1;")
                        mo = re.search(r"<td.*?>(.*?)</td>", str(out_o), flags=re.S | re.I)
                        if mo:
                            original_table = re.sub(r"<.*?>", "", mo.group(1)).strip()
                    except Exception:
                        pass

                if not backup_table:
                    kernel._send_message("stderr", "No backup table found in metadata for rollback token.")
                    return

                # perform atomic restore: backup_table -> original_table
                lock_name = f"fillmissing_rb_{token}"
                self._acquire_lock(mariadb_client, lock_name, timeout=10)
                try:
                    if original_table:
                        if self._table_exists(mariadb_client, original_table):
                            original_old = f"{original_table}_prerollback_{token}"
                            mariadb_client.run_statement(f"RENAME TABLE {original_table} TO {original_old}, {backup_table} TO {original_table};")
                            if mariadb_client.iserror():
                                kernel._send_message("stderr", "Failed to rename tables during rollback (check permissions).")
                                return
                            kernel._send_message("stdout", f"Rollback: restored {backup_table} -> {original_table}; previous {original_table} renamed to {original_old}.")
                            self._insert_metadata(
                                kernel=kernel,
                                command_name=self.name(),
                                arguments=self.args if isinstance(self.args, str) else str(self.args),
                                affected_columns='\n'.join(target_columns) if target_columns else 'ALL_COLUMNS',
                                operation_status='rollback',
                                message=f'restored_to={original_table};previous_saved_as={original_old}',
                                db_name=db_name,
                                user_name=user_name,
                                rollback_token=token,
                                backup_table=backup_table,
                                original_table=original_table
                            )
                        else:
                            mariadb_client.run_statement(f"RENAME TABLE {backup_table} TO {original_table};")
                            if mariadb_client.iserror():
                                kernel._send_message("stderr", "Failed to rename backup to original during rollback (check permissions).")
                                return
                            kernel._send_message("stdout", f"Rollback: renamed {backup_table} -> {original_table}.")
                            self._insert_metadata(
                                kernel=kernel,
                                command_name=self.name(),
                                arguments=self.args if isinstance(self.args, str) else str(self.args),
                                affected_columns='\n'.join(target_columns) if target_columns else 'ALL_COLUMNS',
                                operation_status='rollback',
                                message=f'restored_to={original_table}',
                                db_name=db_name,
                                user_name=user_name,
                                rollback_token=token,
                                backup_table=backup_table,
                                original_table=original_table
                            )
                    else:
                        # try to infer original table name from arguments
                        out_args = mariadb_client.run_statement(f"SELECT arguments FROM {db_name}.magic_metadata WHERE rollback_token={self._sql_escape(token)} LIMIT 1;")
                        margs = re.search(r"<td.*?>(.*?)</td>", str(out_args), flags=re.S | re.I)
                        inferred_original = None
                        if margs:
                            args_txt = re.sub(r"<.*?>", "", margs.group(1)).strip()
                            mm = re.search(r"table\s*=\s*([^\s,]+)", args_txt)
                            if mm:
                                inferred_original = mm.group(1).strip()
                        if inferred_original:
                            if self._table_exists(mariadb_client, inferred_original):
                                original_old = f"{inferred_original}_prerollback_{token}"
                                mariadb_client.run_statement(f"RENAME TABLE {inferred_original} TO {original_old}, {backup_table} TO {inferred_original};")
                                if mariadb_client.iserror():
                                    kernel._send_message("stderr", "Failed to rename during rollback (check permissions).")
                                    return
                                kernel._send_message("stdout", f"Rollback: restored {backup_table} -> {inferred_original}; previous {inferred_original} renamed to {original_old}.")
                                self._insert_metadata(
                                    kernel=kernel,
                                    command_name=self.name(),
                                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                                    affected_columns='\n'.join(target_columns) if target_columns else 'ALL_COLUMNS',
                                    operation_status='rollback',
                                    message=f'restored_to={inferred_original};previous_saved_as={original_old}',
                                    db_name=db_name,
                                    user_name=user_name,
                                    rollback_token=token,
                                    backup_table=backup_table,
                                    original_table=inferred_original
                                )
                            else:
                                mariadb_client.run_statement(f"RENAME TABLE {backup_table} TO {inferred_original};")
                                if mariadb_client.iserror():
                                    kernel._send_message("stderr", "Failed to rename backup to inferred original during rollback (check permissions).")
                                    return
                                kernel._send_message("stdout", f"Rollback: renamed {backup_table} -> {inferred_original}.")
                                self._insert_metadata(
                                    kernel=kernel,
                                    command_name=self.name(),
                                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                                    affected_columns='\n'.join(target_columns) if target_columns else 'ALL_COLUMNS',
                                    operation_status='rollback',
                                    message=f'restored_to={inferred_original}',
                                    db_name=db_name,
                                    user_name=user_name,
                                    rollback_token=token,
                                    backup_table=backup_table,
                                    original_table=inferred_original
                                )
                        else:
                            kernel._send_message("stderr", "Could not determine original table name for rollback. Manual restoration required.")
                            return
                finally:
                    self._release_lock(mariadb_client, lock_name)
            except Exception as e:
                kernel._send_message("stderr", f"Rollback error: {e}")
            return

        # --- APPLY MODE -----------------------------------------------
        if mode == "apply":
            # DB-target apply if table provided and mariadb_client present
            if table_full and mariadb_client is not None:
                # require explicit confirmation for DB changes
                if not confirm:
                    kernel._send_message("stderr", "DB apply requires confirm=true to proceed. Preview first, then re-run with confirm=true.")
                    return

                # we'll compute fill-values per column (best-effort), construct a CTAS where we apply COALESCE(column, <fill_val>)
                token = str(uuid.uuid4()).replace('-', '')[:16]
                backup_table = f"{table_full}_backup_{token}"
                new_table = f"{table_full}_vnew_{token}"
                fill_map = {}  # col -> (ok, val, msg)

                for col in target_columns:
                    ok, val, msg = self._compute_fill_value_db(mariadb_client, table_full, col, strategy, const_value, sample_size=sample_size)
                    fill_map[col] = (ok, val, msg)
                    if not ok:
                        kernel._send_message("stdout", f"Column '{col}': could not compute fill value ({msg}) — will skip filling this column in DB apply.")

                # Build select expressions: for columns we can fill use COALESCE(col, <val>) AS col; for others keep col
                exprs = []
                for c in list(df.columns):
                    if c in fill_map and fill_map[c][0]:
                        val = fill_map[c][1]
                        # decide whether to quote: try numeric conversion
                        try:
                            # allow numeric literal if val is a number
                            if isinstance(val, (int, float)):
                                literal = str(val)
                            else:
                                # attempt to parse numeric-like string
                                literal = str(val)
                                # try to parse float
                                try:
                                    float(literal)
                                    literal = literal
                                except Exception:
                                    literal = self._sql_escape(literal)
                        except Exception:
                            literal = self._sql_escape(str(val))
                        # If literal looks already quoted (i.e. started with '), use directly
                        if isinstance(literal, str) and literal.startswith("'") and literal.endswith("'"):
                            exprs.append(f"COALESCE({c}, {literal}) AS {c}")
                        else:
                            # numeric or unquoted string (we still need to ensure strings are quoted)
                            try:
                                # if numeric
                                float(literal)
                                exprs.append(f"COALESCE({c}, {literal}) AS {c}")
                            except Exception:
                                exprs.append(f"COALESCE({c}, {self._sql_escape(literal)}) AS {c}")
                    else:
                        exprs.append(c)

                select_expr = ", ".join(exprs)
                try:
                    lock_name = f"fillmissing_apply_{token}"
                    got_lock = self._acquire_lock(mariadb_client, lock_name, timeout=10)
                    if not got_lock:
                        kernel._send_message("stderr", "Could not acquire advisory lock; aborting apply.")
                        return

                    # create new table with filled values
                    mariadb_client.run_statement(f"CREATE TABLE {new_table} AS SELECT {select_expr} FROM {table_full};")
                    if mariadb_client.iserror():
                        kernel._send_message("stderr", "Failed to create new table for apply (CTAS failed).")
                        return

                    # atomic rename original -> backup, new -> original
                    mariadb_client.run_statement(f"RENAME TABLE {table_full} TO {backup_table}, {new_table} TO {table_full};")
                    if mariadb_client.iserror():
                        kernel._send_message("stderr", "RENAME TABLE failed (apply may be inconsistent).")
                        return

                    kernel._send_message("stdout", f"Apply completed: original preserved as {backup_table}.")
                    # Insert metadata with token so rollback can restore
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns='\n'.join(target_columns) if target_columns else 'ALL_COLUMNS',
                        operation_status='applied',
                        message=f'applied_backup={backup_table}',
                        db_name=db_name,
                        user_name=user_name,
                        rollback_token=token,
                        backup_table=backup_table,
                        original_table=table_full
                    )

                    # attempt to refresh last_select with a sample
                    try:
                        mariadb_client.run_statement(f"SELECT * FROM {table_full} LIMIT {sample_size};")
                        fresh = mariadb_client.run_statement(f"SELECT * FROM {table_full} LIMIT {sample_size};")
                        try:
                            df_list = pd.read_html(fresh)
                            if df_list and len(df_list) > 0:
                                data["last_select"] = df_list[0]
                                try:
                                    self._send_html(kernel, data["last_select"])
                                except Exception:
                                    pass
                        except Exception:
                            kernel._send_message("stdout", "Applied to DB; could not refresh last_select from DB.")
                    except Exception:
                        pass

                except Exception as e:
                    kernel._send_message("stderr", f"Apply (DB versioned) failed: {e}")
                    log.exception(e)
                finally:
                    self._release_lock(mariadb_client, lock_name)
                return

            else:
                # Local in-place apply on data['last_select'] (behaves like original implementation)
                operation_status = "success"
                messages = []
                try:
                    before_count = len(df)
                    for col in target_columns:
                        try:
                            series = df[col]
                            if strategy in {"mean", "median"}:
                                if pd.api.types.is_numeric_dtype(series):
                                    if strategy == "mean":
                                        fill_val = series.mean(skipna=True)
                                    else:
                                        fill_val = series.median(skipna=True)
                                    if pd.isna(fill_val):
                                        messages.append(f"Column '{col}': no non-missing values to compute {strategy}. Skipped.")
                                        continue
                                    df[col].fillna(fill_val, inplace=True)
                                    messages.append(f"Column '{col}': filled missing with {strategy}={fill_val}.")
                                else:
                                    messages.append(f"Column '{col}' is not numeric; cannot use {strategy}. Skipped.")
                                    continue
                            elif strategy == "mode":
                                modes = series.mode(dropna=True)
                                if modes.empty:
                                    messages.append(f"Column '{col}': no mode (all missing). Skipped.")
                                    continue
                                fill_val = modes.iloc[0]
                                df[col].fillna(fill_val, inplace=True)
                                messages.append(f"Column '{col}': filled missing with mode={fill_val}.")
                            elif strategy == "constant":
                                fill_val = const_value
                                df[col].fillna(fill_val, inplace=True)
                                messages.append(f"Column '{col}': filled missing with constant value={fill_val}.")
                        except Exception as e:
                            operation_status = "error"
                            messages.append(f"Column '{col}': error while filling missing values: {e}")

                    after_count = len(df)
                    dropped = 0  # not relevant here
                    data["last_select"] = df
                    summary = "\n".join(messages)
                    kernel._send_message("stdout", f"Fill missing completed (in-place). Summary:\n{summary}")
                    try:
                        self._send_html(kernel, df)
                    except Exception:
                        pass
                except Exception as e:
                    operation_status = "error"
                    kernel._send_message("stderr", f"Error while applying fillmissing locally: {e}")
                    messages.append(f"Error while applying fillmissing locally: {e}")

                # Insert metadata
                try:
                    args_for_db = self.args if isinstance(self.args, str) else str(self.args)
                    affected_columns_str = "\n".join(target_columns) if target_columns else ""
                    message_str = "\n".join(messages)
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=args_for_db,
                        affected_columns=affected_columns_str,
                        operation_status=operation_status,
                        message=message_str,
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    try:
                        kernel._send_message("stdout", "Warning: failed to write metadata (continuing).")
                    except Exception:
                        pass

                return

        # fallback
        kernel._send_message("stderr", "Unknown execution path reached.")
        return
