# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.maria_magic import MariaMagic
import shlex
from distutils import util
import pandas as pd
import numpy as np
from collections import namedtuple
import logging
import os
import re
import uuid
import time

# Optional helper to reliably get current DB name (if available in environment)
try:
    from mariadb_kernel.sql_fetch import SqlFetch
except Exception:
    SqlFetch = None


class DropOutliers(MariaMagic):
    """
    %dropoutliers [columns=col1,col2,...] [method=iqr|zscore] [k=1.5] [z_thresh=3.0]
                  [mode=preview|apply|rollback] [table=schema.table] [confirm=true|false]
                  [sample_size=100] [lock_timeout=10]

    Modes:
      - preview: estimate rows removed, show sample rows
      - apply: perform removal (in-place local or DB CTAS+RENAME if table= provided)
      - rollback: restore DB backup created by apply (requires mariadb_client)

    Notes:
      - DB apply uses sampling to compute thresholds (best-effort).
      - Execution metadata recorded in magic_metadata (includes rollback token).
    """

    def __init__(self, args=""):
        self.args = args

    def type(self):
        return "Line"

    def name(self):
        return "dropoutliers"

    def help(self):
        return (
            "%dropoutliers [columns=col1,col2,...] [method=iqr|zscore] [k=1.5] [z_thresh=3.0]\n"
            "             [mode=preview|apply|rollback] [table=schema.table] [confirm=true|false]\n"
            "             [sample_size=100]\n"
            "Removes rows containing outliers from data['last_select'] or from a DB table (versioned apply).\n"
            "Execution metadata is recorded in table `magic_metadata`."
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

    def _detect_outliers_series(self, series, method, k=1.5, z_thresh=3.0):
        """Return boolean mask of outliers for a pandas Series (True where outlier)."""
        if series.dropna().empty:
            return pd.Series(False, index=series.index)

        if method == "iqr":
            q1 = series.quantile(0.25)
            q3 = series.quantile(0.75)
            iqr = q3 - q1
            lower = q1 - k * iqr
            upper = q3 + k * iqr
            mask = (series < lower) | (series > upper)
            return mask.fillna(False)

        elif method == "zscore":
            mean = series.mean(skipna=True)
            std = series.std(skipna=True)
            if std == 0 or np.isnan(std):
                return pd.Series(False, index=series.index)
            z = (series - mean) / std
            mask = z.abs() > float(z_thresh)
            return mask.fillna(False)

        else:
            raise ValueError(f"Unknown method {method}")

    # --- metadata / DB helper methods (best-effort) ---
    def _get_mariadb_client(self, kernel):
        return getattr(kernel, "mariadb_client", None)

    def _get_logger(self, kernel):
        return getattr(kernel, "log", logging.getLogger(__name__))

    def _sql_escape(self, val):
        """Escape value to single-quoted SQL literal (None -> NULL)."""
        if val is None:
            return "NULL"
        if not isinstance(val, str):
            val = str(val)
        return "'" + val.replace("'", "''") + "'"

    def _get_db_name(self, kernel):
        """
        Attempt to determine current DB. Use SqlFetch if present; otherwise run SELECT DATABASE(); parse result.
        Returns empty string if none.
        """
        mariadb_client = self._get_mariadb_client(kernel)
        log = self._get_logger(kernel)

        if SqlFetch is not None and mariadb_client is not None:
            try:
                sf = SqlFetch(mariadb_client, log)
                return sf.get_db_name() or ""
            except Exception:
                log.debug("SqlFetch.get_db_name() failed; falling back to manual query.")

        if mariadb_client is None:
            return ""
        try:
            res = mariadb_client.run_statement("SELECT DATABASE();")
            if mariadb_client.iserror():
                return ""
            if not res:
                return ""
            # try parsing HTML table via pandas
            try:
                dfs = pd.read_html(res)
                if dfs and len(dfs) > 0:
                    val = dfs[0].iloc[0, 0]
                    if isinstance(val, float) and pd.isna(val):
                        return ""
                    return str(val) if val is not None else ""
            except Exception:
                # fallback: regex extract first td
                m = re.search(r"<td.*?>(.*?)</td>", str(res), flags=re.S | re.I)
                if m:
                    txt = re.sub(r"<.*?>", "", m.group(1)).strip()
                    if txt.lower() == "null" or txt == "":
                        return ""
                    return txt
                txt = str(res).strip()
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
                         operation_status, message, db_name, user_name,
                         rollback_token=None, backup_table=None, original_table=None):
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

    def _compute_thresholds_db(self, mariadb_client, table_full, col, method, k=1.5, z_thresh=3.0, sample_size=100):
        """
        Sample non-null values from DB and compute thresholds for IQR or zscore.
        Returns (ok, {lower:.., upper:..}, message)
        """
        try:
            out = mariadb_client.run_statement(f"SELECT {col} FROM {table_full} WHERE {col} IS NOT NULL LIMIT {int(sample_size)};")
            if mariadb_client.iserror() or not out:
                return False, None, "sample query failed"
            try:
                df_list = pd.read_html(out)
                if not df_list or len(df_list) == 0:
                    return False, None, "no sample rows parsed"
                series = df_list[0].iloc[:, 0].astype(float)  # try numeric conversion
                if series.dropna().empty:
                    return False, None, "sample contains no numeric values"
                if method == "iqr":
                    q1 = series.quantile(0.25)
                    q3 = series.quantile(0.75)
                    iqr = q3 - q1
                    lower = q1 - k * iqr
                    upper = q3 + k * iqr
                    return True, {"lower": float(lower), "upper": float(upper)}, "iqr via sampling"
                elif method == "zscore":
                    mean = float(series.mean())
                    std = float(series.std())
                    if std == 0:
                        return False, None, "std==0 in sample"
                    lower = mean - float(z_thresh) * std
                    upper = mean + float(z_thresh) * std
                    return True, {"lower": float(lower), "upper": float(upper)}, "zscore via sampling"
                else:
                    return False, None, "unknown method"
            except Exception:
                # fallback regex parse single-column HTML
                vals = re.findall(r"<td.*?>(.*?)</td>", str(out), flags=re.S | re.I)
                nums = []
                for v in vals:
                    txt = re.sub(r"<.*?>", "", v).strip()
                    try:
                        nums.append(float(txt))
                    except Exception:
                        continue
                if not nums:
                    return False, None, "parsed sample contains no numeric values"
                series = pd.Series(nums)
                if method == "iqr":
                    q1 = series.quantile(0.25)
                    q3 = series.quantile(0.75)
                    iqr = q3 - q1
                    lower = q1 - k * iqr
                    upper = q3 + k * iqr
                    return True, {"lower": float(lower), "upper": float(upper)}, "iqr via regex sample"
                elif method == "zscore":
                    mean = float(series.mean())
                    std = float(series.std())
                    if std == 0:
                        return False, None, "std==0 in sample"
                    lower = mean - float(z_thresh) * std
                    upper = mean + float(z_thresh) * std
                    return True, {"lower": float(lower), "upper": float(upper)}, "zscore via regex sample"
                else:
                    return False, None, "unknown method"
        except Exception as e:
            return False, None, f"exception computing thresholds: {e}"

    def _parse_count_result(self, res):
        """Parse a SELECT COUNT(*) result returned by mariadb_client.run_statement (HTML or text)."""
        try:
            df_list = pd.read_html(res)
            if df_list and len(df_list) > 0:
                val = df_list[0].iloc[0, 0]
                try:
                    return int(val)
                except Exception:
                    try:
                        return int(float(val))
                    except Exception:
                        return None
        except Exception:
            m = re.search(r"<td.*?>(.*?)</td>", str(res), flags=re.S | re.I)
            if m:
                txt = re.sub(r"<.*?>", "", m.group(1)).strip()
                try:
                    return int(txt)
                except Exception:
                    try:
                        return int(float(txt))
                    except Exception:
                        return None
        # fallback: try to parse raw
        try:
            txt = str(res).strip()
            return int(txt)
        except Exception:
            try:
                return int(float(str(res)))
            except Exception:
                return None

    def execute(self, kernel, data):
        """Execute the dropoutliers magic (preview/apply/rollback)."""
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
            columns = [c.strip() for c in columns_arg.split(",") if c.strip()]
        elif isinstance(columns_arg, (list, tuple)):
            columns = list(columns_arg)
        else:
            columns = None

        # method and params
        method = str(args.get("method", "iqr")).lower()
        if method not in {"iqr", "zscore"}:
            kernel._send_message("stderr", f"Unknown method '{method}'. Allowed: iqr, zscore.")
            return

        try:
            k = float(args.get("k", 1.5))
        except Exception:
            k = 1.5

        try:
            z_thresh = float(args.get("z_thresh", 3.0))
        except Exception:
            z_thresh = 3.0

        # mode
        mode = str(args.get("mode", "preview")).lower()
        mode = mode if mode in {"preview", "apply", "rollback"} else "preview"

        table_full = args.get("table", None)
        confirm = bool(args.get("confirm", False))
        sample_size = int(args.get("sample_size", 100))
        lock_timeout = int(args.get("lock_timeout", 10))

        mariadb_client = self._get_mariadb_client(kernel)
        log = self._get_logger(kernel)
        db_name = self._get_db_name(kernel)
        user_name = self._get_user_name(kernel)

        # Determine target numeric columns
        if columns is None:
            target_columns = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        else:
            missing_cols = [c for c in columns if c not in df.columns]
            if missing_cols:
                kernel._send_message("stderr", f"Column(s) not found: {', '.join(missing_cols)}")
                # log metadata for failure and return
                try:
                    self._ensure_metadata_table(kernel, db_name)
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns="\n".join(columns) if columns else "",
                        operation_status="error",
                        message=f"Column(s) not found: {', '.join(missing_cols)}",
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    pass
                return
            # keep only numeric columns
            target_columns = [c for c in columns if pd.api.types.is_numeric_dtype(df[c])]
            non_numeric = [c for c in columns if c not in target_columns]
            if non_numeric:
                kernel._send_message("stdout", f"Warning: non-numeric columns skipped: {', '.join(non_numeric)}")

        if not target_columns:
            kernel._send_message("stderr", "No numeric target columns found to detect outliers.")
            # log metadata for early exit
            try:
                self._ensure_metadata_table(kernel, db_name)
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns="",
                    operation_status="error",
                    message="No numeric target columns found to detect outliers.",
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return

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
                # local detection counts & sample
                messages = []
                combined_mask = None
                for col in target_columns:
                    mask = self._detect_outliers_series(df[col], method, k=k, z_thresh=z_thresh)
                    n_out = int(mask.sum())
                    messages.append(f"Column '{col}': detected {n_out} outlier(s) using {method}.")
                    if combined_mask is None:
                        combined_mask = mask.astype(bool)
                    else:
                        combined_mask = combined_mask | mask.astype(bool)

                n_before = len(df)
                n_after = n_before - (int(combined_mask.sum()) if combined_mask is not None else 0)
                kernel._send_message("stdout", f"PREVIEW (local): would drop {n_before - n_after} row(s) (from {n_before} to {n_after}).\n" + "\n".join(messages))

                # show sample rows that would be dropped (local)
                if combined_mask is not None and combined_mask.any():
                    sample_rows = df[combined_mask].head(sample_size).copy()
                    sample_rows["_outlier_cols"] = sample_rows.apply(lambda r: ",".join([c for c in target_columns if pd.isnull(r.get(c)) is False and self._detect_outliers_series(pd.Series([r.get(c)]*1), method, k=k, z_thresh=z_thresh).iloc[0]]), axis=1)
                    try:
                        self._send_html(kernel, sample_rows)
                    except Exception:
                        kernel._send_message("stdout", str(sample_rows.head()))
                else:
                    kernel._send_message("stdout", "PREVIEW (local): no rows with outliers in the sample.")

                # If DB target provided, attempt DB-based estimate (using sampling thresholds)
                if table_full and mariadb_client is not None:
                    db_messages = []
                    predicates = []
                    for col in target_columns:
                        ok, thresholds, msg = self._compute_thresholds_db(mariadb_client, table_full, col, method, k=k, z_thresh=z_thresh, sample_size=sample_size)
                        if ok and thresholds:
                            lower = thresholds["lower"]
                            upper = thresholds["upper"]
                            # ensure numeric literal formatting
                            predicates.append(f"({col} < {repr(lower)} OR {col} > {repr(upper)})")
                            db_messages.append(f"{col}: thresholds approx [{lower}, {upper}] ({msg})")
                        else:
                            db_messages.append(f"{col}: could not compute thresholds ({msg}) - skipped in DB predicate")

                    if predicates:
                        db_pred = " OR ".join(predicates)
                        try:
                            out = mariadb_client.run_statement(f"SELECT COUNT(*) FROM {table_full} WHERE {db_pred};")
                            cnt = self._parse_count_result(out)
                            if cnt is None:
                                kernel._send_message("stdout", "PREVIEW (db): could not parse count result (check permissions).")
                            else:
                                kernel._send_message("stdout", f"PREVIEW (db): estimated rows matching outlier predicate: {cnt}.")
                        except Exception:
                            kernel._send_message("stdout", "PREVIEW (db): failed to run count query (continuing).")
                        kernel._send_message("stdout", "PREVIEW (db) thresholds:\n" + "\n".join(db_messages))
                    else:
                        kernel._send_message("stdout", "PREVIEW (db): no DB predicates could be computed (insufficient sample/values).")
                # log preview metadata
                try:
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns='\n'.join(target_columns),
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
                    # try to find latest rollback_token for this command + user
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

                # get backup_table and original_table
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
                lock_name = f"dropoutliers_rb_{token}"
                self._acquire_lock(mariadb_client, lock_name, timeout=lock_timeout)
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
                                affected_columns='\n'.join(target_columns),
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
                                affected_columns='\n'.join(target_columns),
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
                                    affected_columns='\n'.join(target_columns),
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
                                    affected_columns='\n'.join(target_columns),
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
                predicates = []
                messages = []

                # compute thresholds for each column using DB sampling
                for col in target_columns:
                    ok, thresholds, msg = self._compute_thresholds_db(mariadb_client, table_full, col, method, k=k, z_thresh=z_thresh, sample_size=sample_size)
                    if ok and thresholds:
                        lower = thresholds["lower"]
                        upper = thresholds["upper"]
                        # use repr to keep decimal representation
                        predicates.append(f"({col} < {repr(lower)} OR {col} > {repr(upper)})")
                        messages.append(f"{col}: thresholds [{lower}, {upper}] ({msg})")
                    else:
                        messages.append(f"{col}: could not compute thresholds ({msg}); this column will not be used in DB predicate")

                if not predicates:
                    kernel._send_message("stderr", "Could not compute DB predicates for any column; aborting DB apply.")
                    try:
                        self._insert_metadata(
                            kernel=kernel,
                            command_name=self.name(),
                            arguments=self.args if isinstance(self.args, str) else str(self.args),
                            affected_columns='\n'.join(target_columns),
                            operation_status='error',
                            message='db_apply_failed_no_predicates',
                            db_name=db_name,
                            user_name=user_name
                        )
                    except Exception:
                        pass
                    return

                db_pred = " OR ".join(predicates)

                try:
                    lock_name = f"dropoutliers_apply_{token}"
                    got_lock = self._acquire_lock(mariadb_client, lock_name, timeout=lock_timeout)
                    if not got_lock:
                        kernel._send_message("stderr", "Could not acquire advisory lock; aborting apply.")
                        return

                    # create new table with rows to keep (NOT predicate)
                    mariadb_client.run_statement(f"CREATE TABLE {new_table} AS SELECT * FROM {table_full} WHERE NOT ({db_pred});")
                    if mariadb_client.iserror():
                        kernel._send_message("stderr", "Failed to create new table for apply (CTAS failed).")
                        return

                    # atomic rename original -> backup, new -> original
                    mariadb_client.run_statement(f"RENAME TABLE {table_full} TO {backup_table}, {new_table} TO {table_full};")
                    if mariadb_client.iserror():
                        kernel._send_message("stderr", "RENAME TABLE failed (apply may be inconsistent).")
                        return

                    kernel._send_message("stdout", f"Apply completed: original preserved as {backup_table}.")
                    # log metadata with rollback token so rollback can restore
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns='\n'.join(target_columns),
                        operation_status='applied',
                        message='applied_db_versioned',
                        db_name=db_name,
                        user_name=user_name,
                        rollback_token=token,
                        backup_table=backup_table,
                        original_table=table_full
                    )

                    # attempt to refresh last_select with sample
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
                combined_mask = None
                messages = []
                operation_status = "success"
                try:
                    for col in target_columns:
                        try:
                            mask = self._detect_outliers_series(df[col], method, k=k, z_thresh=z_thresh)
                            n_out = int(mask.sum())
                            messages.append(f"Column '{col}': detected {n_out} outlier(s) using {method}.")
                            if combined_mask is None:
                                combined_mask = mask.astype(bool)
                            else:
                                combined_mask = combined_mask | mask.astype(bool)
                        except Exception as e:
                            operation_status = "error"
                            messages.append(f"Column '{col}': error detecting outliers: {e}")

                    if combined_mask is None or not combined_mask.any():
                        kernel._send_message("stdout", "No outliers detected. No rows removed.\n" + "\n".join(messages))
                        try:
                            self._send_html(kernel, df)
                        except Exception:
                            pass
                    else:
                        n_before = len(df)
                        df.drop(index=df[combined_mask].index, inplace=True)
                        data["last_select"] = df
                        n_after = len(df)
                        removed = n_before - n_after
                        kernel._send_message("stdout", f"Dropped {removed} row(s) containing outliers (in-place).\n" + "\n".join(messages))
                        try:
                            self._send_html(kernel, df)
                        except Exception:
                            pass

                except Exception as e:
                    operation_status = "error"
                    kernel._send_message("stderr", f"Error while removing outlier rows locally: {e}")
                    messages.append(str(e))

                # Insert metadata
                try:
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns="\n".join(target_columns),
                        operation_status=operation_status,
                        message="\n".join(messages),
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
