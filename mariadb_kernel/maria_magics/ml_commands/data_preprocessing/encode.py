# encode.py
# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.maria_magic import MariaMagic
import pandas as pd
import shlex
from distutils import util
import numpy as np
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder
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


class Encode(MariaMagic):
    """
    %encode method=<label|onehot|ordinal>
            [columns=col1,col2,...]
            [inplace=true|false]
            [drop_original=true|false]
            [mode=preview|apply|rollback]
            [table=schema.table] [confirm=true|false] [sample_size=100]

    Notes:
     - If columns omitted, object/category dtype columns are auto-selected.
     - Default: inplace=true, drop_original=true.
     - DB apply uses CTAS + atomic RENAME and records rollback metadata.
    """

    def __init__(self, args=""):
        self.args = args

    def type(self):
        return "Line"

    def name(self):
        return "encode"

    def help(self):
        return (
            "%encode method=<label|onehot|ordinal> [columns=col1,col2] "
            "[inplace=true] [drop_original=true] [mode=preview|apply|rollback]\n"
            "[table=schema.table] [confirm=true] [sample_size=100]\n"
            "Encode categorical columns. Preview shows what will be created. "
            "Apply can operate locally or on a DB table (versioned)."
        )

    def _str_to_obj(self, s):
        """Cast to int/float/bool when possible, otherwise return string."""
        try:
            return int(s)
        except (ValueError, TypeError):
            pass
        try:
            return float(s)
        except (ValueError, TypeError):
            pass
        try:
            return bool(util.strtobool(str(s)))
        except Exception:
            return s

    def parse_args(self, input_str):
        if not input_str or input_str.strip() == "":
            return {}
        pairs = dict(token.split("=", 1) for token in shlex.split(input_str))
        for k, v in pairs.items():
            pairs[k] = self._str_to_obj(v)
        return pairs

    def _send_html(self, kernel, df):
        try:
            html = df.to_html(index=False)
            mime = "text/html"
        except Exception:
            html = str(df)
            mime = "text/plain"
        display_content = {"data": {mime: html}, "metadata": {}}
        kernel.send_response(kernel.iopub_socket, "display_data", display_content)

    def _make_ohe(self, **kwargs):
        """
        Create OneHotEncoder in a sklearn-version compatible way.
        Older sklearn versions accept `sparse`; newer use `sparse_output`.
        """
        try:
            return OneHotEncoder(sparse=False, **kwargs)
        except TypeError:
            return OneHotEncoder(sparse_output=False, **kwargs)

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
        return "'" + val.replace("'", "''") + "'"

    def _safe_colname(self, s):
        """Create a safe column identifier from an arbitrary string."""
        if s is None:
            return ""
        s2 = re.sub(r"[^0-9A-Za-z_]", "_", str(s))
        # ensure not starting with digit
        if re.match(r"^[0-9]", s2):
            s2 = "_" + s2
        return s2[:200]  # cap length

    def _get_db_name(self, kernel):
        """
        Attempt to determine the currently used DB.
        Prefer SqlFetch if available; otherwise run SELECT DATABASE(); and try to parse.
        Returns empty string if none found.
        """
        mariadb_client = self._get_mariadb_client(kernel)
        log = self._get_logger(kernel)

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
            if mariadb_client.iserror():
                return ""
            if not result:
                return ""
            # Try to parse HTML table with pandas
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
        Includes rollback support columns (rollback_token, backup_table, original_table).
        """
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

    def _parse_distinct_results(self, result):
        """Return list of values from a run_statement output (HTML or plain)."""
        if not result:
            return []
        try:
            dfs = pd.read_html(result)
            if dfs and len(dfs) > 0:
                series = dfs[0].iloc[:, 0].astype(object)
                return [None if (pd.isna(x) or x is None) else x for x in series.tolist()]
        except Exception:
            vals = re.findall(r"<td.*?>(.*?)</td>", str(result), flags=re.S | re.I)
            parsed = []
            for v in vals:
                txt = re.sub(r"<.*?>", "", v).strip()
                if txt.lower() == "null":
                    parsed.append(None)
                else:
                    parsed.append(txt)
            if parsed:
                return parsed
        # Last fallback: attempt to split raw text lines
        try:
            txt = str(result).strip()
            lines = [l.strip() for l in txt.splitlines() if l.strip()]
            return lines
        except Exception:
            return []

    # -------------------- end metadata helpers --------------------

    def execute(self, kernel, data):
        # get DataFrame
        df = data.get("last_select")
        if df is None:
            kernel._send_message("stderr", "No last_select found in kernel data.")
            return
        if hasattr(df, "empty") and df.empty:
            kernel._send_message("stderr", "There is no data to encode (empty DataFrame).")
            return

        try:
            args = self.parse_args(self.args)
        except Exception:
            kernel._send_message("stderr", "Error parsing arguments.")
            return

        method = str(args.get("method", "label")).lower()
        cols_arg = args.get("columns", None)
        if isinstance(cols_arg, str):
            columns = [c.strip() for c in cols_arg.split(",") if c.strip()]
        elif isinstance(cols_arg, (list, tuple)):
            columns = list(cols_arg)
        else:
            columns = list(df.select_dtypes(include=["object", "category"]).columns)

        if not columns:
            kernel._send_message("stderr", "No columns specified or detected for encoding.")
            # log metadata for failure
            try:
                db_name = self._get_db_name(kernel)
                user_name = self._get_user_name(kernel)
                self._ensure_metadata_table(kernel, db_name)
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns="",
                    operation_status="error",
                    message="No columns specified or detected for encoding.",
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return

        # validate existence
        missing_cols = [c for c in columns if c not in df.columns]
        if missing_cols:
            msg = f"Column(s) not found: {', '.join(missing_cols)}"
            kernel._send_message("stderr", msg)
            # log metadata for failure
            try:
                db_name = self._get_db_name(kernel)
                user_name = self._get_user_name(kernel)
                self._ensure_metadata_table(kernel, db_name)
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns="\n".join(columns),
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return

        inplace = bool(args.get("inplace", True))
        drop_original = bool(args.get("drop_original", True))

        # mode and db args
        mode = str(args.get("mode", "preview")).lower()
        mode = mode if mode in {"preview", "apply", "rollback"} else "preview"
        table_full = args.get("table", None)
        confirm = bool(args.get("confirm", False))
        sample_size = int(args.get("sample_size", 100))

        mariadb_client = self._get_mariadb_client(kernel)
        log = self._get_logger(kernel)
        db_name = self._get_db_name(kernel)
        user_name = self._get_user_name(kernel)

        # ensure metadata table exists
        try:
            self._ensure_metadata_table(kernel, db_name)
        except Exception:
            try:
                kernel._send_message("stdout", "Warning: failed to ensure metadata table (continuing).")
            except Exception:
                pass

        # --- PREVIEW MODE ---
        if mode == "preview":
            try:
                messages = []
                created_columns = []
                # Local preview: compute unique counts, sample mappings
                for col in columns:
                    series = df[col]
                    uniques = pd.Index(series.dropna().unique())
                    n_uniques = len(uniques)
                    messages.append(f"Local: Column '{col}' unique non-null values: {n_uniques} (showing up to 10): {list(uniques[:10])}")
                    if method == "label" or method == "ordinal":
                        created_columns.append(f"{col}_lbl" if method == "label" else f"{col}_ord")
                    elif method == "onehot":
                        # onehot creates one column per category
                        for v in list(uniques[:100]):  # cap for preview listing
                            created_columns.append(f"{col}_{self._safe_colname(v)}")

                kernel._send_message("stdout", "PREVIEW (local):\n" + "\n".join(messages))
                kernel._send_message("stdout", f"PREVIEW (local) estimated created columns: {len(created_columns)}")

                # Show sample rows that will be modified (where any column is not-na)
                sample_mask = pd.Series(False, index=df.index)
                for col in columns:
                    sample_mask = sample_mask | df[col].notna()
                sample_rows = df[sample_mask].head(sample_size)
                if not sample_rows.empty:
                    try:
                        self._send_html(kernel, sample_rows)
                    except Exception:
                        kernel._send_message("stdout", str(sample_rows.head()))

                # DB preview if requested
                if table_full and mariadb_client is not None:
                    db_msgs = []
                    total_estimated_new_cols = 0
                    for col in columns:
                        try:
                            out = mariadb_client.run_statement(f"SELECT DISTINCT {col} FROM {table_full} LIMIT {sample_size};")
                            vals = self._parse_distinct_results(out)
                            nvals = len(vals)
                            db_msgs.append(f"DB: Column '{col}' distinct values (up to {sample_size}): {vals[:10]} (count_est={nvals})")
                            if method == "label" or method == "ordinal":
                                total_estimated_new_cols += 1
                            else:
                                total_estimated_new_cols += nvals
                        except Exception as e:
                            db_msgs.append(f"DB: Column '{col}' distinct query failed: {e}")
                    kernel._send_message("stdout", "PREVIEW (db):\n" + "\n".join(db_msgs))
                    kernel._send_message("stdout", f"PREVIEW (db) estimated created columns: {total_estimated_new_cols}")

                # log preview metadata
                try:
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns="\n".join(columns),
                        operation_status='preview',
                        message='preview_completed',
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    pass

            except Exception as e:
                kernel._send_message("stderr", f"Error during preview: {e}")
            return

        # --- ROLLBACK MODE ---
        if mode == "rollback":
            if mariadb_client is None:
                kernel._send_message("stderr", "Rollback requested but no mariadb_client available.")
                return
            token = args.get("rollback_token", None)
            try:
                if not token:
                    # try to read latest magic_metadata entry for this command and user
                    mariadb_client.run_statement(f"SELECT rollback_token FROM {db_name}.magic_metadata WHERE command_name={self._sql_escape(self.name())} AND user_name={self._sql_escape(user_name)} ORDER BY execution_timestamp DESC LIMIT 1;")
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
                lock_name = f"encode_rb_{token}"
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
                                affected_columns='\n'.join(columns),
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
                                affected_columns='\n'.join(columns),
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
                                    affected_columns='\n'.join(columns),
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
                                    affected_columns='\n'.join(columns),
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

        # --- APPLY MODE ---
        if mode == "apply":
            # DB-target apply if table provided and mariadb_client present
            if table_full and mariadb_client is not None:
                if not confirm:
                    kernel._send_message("stderr", "DB apply requires confirm=true to proceed. Preview first, then re-run with confirm=true.")
                    return

                token = str(uuid.uuid4()).replace('-', '')[:16]
                backup_table = f"{table_full}_backup_{token}"
                new_table = f"{table_full}_vnew_{token}"

                # gather distinct values per column from DB (best-effort)
                col_values = {}
                messages = []
                for col in columns:
                    try:
                        out = mariadb_client.run_statement(f"SELECT DISTINCT {col} FROM {table_full} LIMIT {sample_size};")
                        vals = self._parse_distinct_results(out)
                        # we keep the order returned; limit cardinality to avoid explosion
                        col_values[col] = vals
                        messages.append(f"{col}: discovered {len(vals)} distinct values (sample limit {sample_size}).")
                    except Exception as e:
                        col_values[col] = []
                        messages.append(f"{col}: failed to collect distinct values: {e}")

                # Build SELECT expressions
                select_exprs = []
                all_columns = list(df.columns)

                created_cols = []
                created_count = 0

                for c in all_columns:
                    if c in columns:
                        vals = col_values.get(c, [])
                        if method == "label":
                            # build CASE ... WHEN ... THEN idx ELSE NULL END AS col_lbl
                            cases = []
                            for idx, v in enumerate(vals):
                                if v is None:
                                    cases.append(f"WHEN {c} IS NULL THEN {idx}")
                                else:
                                    cases.append(f"WHEN {c} = {self._sql_escape(v)} THEN {idx}")
                            case_sql = " ".join(cases)
                            new_name = f"{c}_lbl"
                            select_exprs.append(f"CASE {case_sql} ELSE NULL END AS {new_name}")
                            created_cols.append(new_name)
                            created_count += 1
                            if not drop_original:
                                select_exprs.append(c)
                        elif method == "ordinal":
                            cases = []
                            for idx, v in enumerate(vals):
                                if v is None:
                                    cases.append(f"WHEN {c} IS NULL THEN {idx}")
                                else:
                                    cases.append(f"WHEN {c} = {self._sql_escape(v)} THEN {idx}")
                            new_name = f"{c}_ord"
                            select_exprs.append(f"CASE {' '.join(cases)} ELSE NULL END AS {new_name}")
                            created_cols.append(new_name)
                            created_count += 1
                            if not drop_original:
                                select_exprs.append(c)
                        elif method == "onehot":
                            # for each distinct value create column col_<safeval> as CASE WHEN col=val THEN 1 ELSE 0 END
                            for v in vals:
                                safe = self._safe_colname(v if v is not None else "NULL")
                                new_name = f"{c}_{safe}"
                                if v is None:
                                    select_exprs.append(f"CASE WHEN {c} IS NULL THEN 1 ELSE 0 END AS {new_name}")
                                else:
                                    select_exprs.append(f"CASE WHEN {c} = {self._sql_escape(v)} THEN 1 ELSE 0 END AS {new_name}")
                                created_cols.append(new_name)
                                created_count += 1
                            if not drop_original:
                                select_exprs.append(c)
                        else:
                            # fallback: keep original
                            select_exprs.append(c)
                    else:
                        # not a targeted column — keep as is
                        select_exprs.append(c)

                # safety cap
                if created_count > 1000:
                    kernel._send_message("stderr", f"Refusing to create {created_count} encoded columns ( > 1000 ). Narrow the columns or reduce distinct values.")
                    try:
                        self._insert_metadata(
                            kernel=kernel,
                            command_name=self.name(),
                            arguments=self.args if isinstance(self.args, str) else str(self.args),
                            affected_columns='\n'.join(columns),
                            operation_status='error',
                            message=f"too_many_created_columns={created_count}",
                            db_name=db_name,
                            user_name=user_name
                        )
                    except Exception:
                        pass
                    return

                select_sql = ", ".join(select_exprs)
                try:
                    lock_name = f"encode_apply_{token}"
                    got_lock = self._acquire_lock(mariadb_client, lock_name, timeout=10)
                    if not got_lock:
                        kernel._send_message("stderr", "Could not acquire advisory lock; aborting apply.")
                        return

                    # create new table with encoded columns
                    mariadb_client.run_statement(f"CREATE TABLE {new_table} AS SELECT {select_sql} FROM {table_full};")
                    if mariadb_client.iserror():
                        kernel._send_message("stderr", "Failed to create new table for apply (CTAS failed).")
                        return

                    # atomic rename original -> backup, new -> original
                    mariadb_client.run_statement(f"RENAME TABLE {table_full} TO {backup_table}, {new_table} TO {table_full};")
                    if mariadb_client.iserror():
                        kernel._send_message("stderr", "RENAME TABLE failed (apply may be inconsistent).")
                        return

                    kernel._send_message("stdout", f"Apply completed: original preserved as {backup_table}.")
                    # log metadata with token so rollback can restore
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns='\n'.join(columns),
                        operation_status='applied',
                        message=f'applied_backup={backup_table};created_columns={created_count}',
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
                # Local in-place apply on data['last_select'] (existing behavior)
                result_df = df if inplace else df.copy()
                messages = []
                operation_status = "success"
                created_columns = []
                try:
                    encoder_obj = None
                    label_mappings = None

                    if method == "label":
                        label_mappings = {}
                        for col in columns:
                            codes, uniques = pd.factorize(result_df[col], sort=True)
                            new_col = f"{col}_lbl"
                            result_df[new_col] = codes
                            created_columns.append(new_col)
                            mapping = {val: idx for idx, val in enumerate(uniques)}
                            label_mappings[col] = mapping
                            if drop_original:
                                result_df.drop(columns=[col], inplace=True)
                            messages.append(f"Column '{col}': label-encoded -> {new_col} (unique_values={len(uniques)})")
                        encoder_obj = label_mappings

                    elif method == "onehot":
                        encoder = self._make_ohe(handle_unknown="ignore")
                        tmp = result_df[columns].astype(object).fillna("___MISSING___")
                        arr = encoder.fit_transform(tmp)
                        try:
                            feature_names = encoder.get_feature_names_out(columns)
                            feature_names = [str(fn) for fn in feature_names]
                        except Exception:
                            cats = encoder.categories_
                            feature_names = []
                            for cname, cat_list in zip(columns, cats):
                                for cat in cat_list:
                                    feature_names.append(f"{cname}_{str(cat)}")
                        ohe_df = pd.DataFrame(arr, columns=feature_names, index=result_df.index)
                        if drop_original:
                            result_df = pd.concat([result_df.drop(columns=columns), ohe_df], axis=1)
                        else:
                            result_df = pd.concat([result_df, ohe_df], axis=1)
                        created_columns.extend(feature_names)
                        messages.append(f"Columns {columns} one-hot encoded -> created {len(feature_names)} columns.")
                        encoder_obj = encoder

                    elif method == "ordinal":
                        enc = OrdinalEncoder(dtype=np.float64)
                        tmp = result_df[columns].astype(object).fillna("___MISSING___")
                        enc_arr = enc.fit_transform(tmp)
                        for i, col in enumerate(columns):
                            new_col = f"{col}_ord"
                            result_df[new_col] = enc_arr[:, i]
                            created_columns.append(new_col)
                            if drop_original:
                                result_df.drop(columns=[col], inplace=True)
                            messages.append(f"Column '{col}': ordinal-encoded -> {new_col}")
                        encoder_obj = enc

                    else:
                        kernel._send_message("stderr", "Unsupported method. Supported: label, onehot, ordinal.")
                        try:
                            self._insert_metadata(
                                kernel=kernel,
                                command_name=self.name(),
                                arguments=self.args if isinstance(self.args, str) else str(self.args),
                                affected_columns="\n".join(columns),
                                operation_status="error",
                                message="Unsupported method requested.",
                                db_name=db_name,
                                user_name=user_name
                            )
                        except Exception:
                            pass
                        return

                    # Apply result back to shared data if inplace
                    if inplace:
                        data["last_select"] = result_df
                        kernel._send_message("stdout", "Encoded columns in-place and updated last_select.")
                    else:
                        kernel._send_message("stdout", "Displayed encoded result (last_select not modified).")

                    # Save encoder (or mapping) to shared data for downstream pipeline usage
                    try:
                        if encoder_obj is not None:
                            data["last_select_encoder"] = encoder_obj
                        elif label_mappings is not None:
                            data["last_select_encoder"] = label_mappings
                    except Exception:
                        pass

                    # display
                    self._send_html(kernel, result_df)

                except Exception as e:
                    operation_status = "error"
                    err_msg = f"Error during encoding: {e}"
                    kernel._send_message("stderr", err_msg)
                    messages.append(err_msg)

                # Attempt to insert metadata (best-effort)
                try:
                    args_for_db = self.args if isinstance(self.args, str) else str(self.args)
                    affected_columns_str = "\n".join(columns)
                    created_columns_str = "\n".join(created_columns) if created_columns else ""
                    details = "\n".join(messages) if messages else "Encoding completed."
                    metadata_message = f"Method: {method}\nCreated columns:\n{created_columns_str}\n\nDetails:\n{details}"
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=args_for_db,
                        affected_columns=affected_columns_str,
                        operation_status=operation_status,
                        message=metadata_message,
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
