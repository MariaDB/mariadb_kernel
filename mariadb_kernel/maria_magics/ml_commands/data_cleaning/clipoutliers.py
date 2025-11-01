# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.
from mariadb_kernel.maria_magics.maria_magic import MariaMagic
import shlex
from distutils import util
import pandas as pd
import numpy as np
from collections import namedtuple
import enum
from typing import Callable, List, NamedTuple, Tuple
import pandas
from pandas.core.frame import DataFrame
# note: we don't strictly rely on SqlFetch import path. We'll attempt to use it if available.
try:
    from mariadb_kernel.sql_fetch import SqlFetch  # optional; used if present
except Exception:
    SqlFetch = None
import logging
import math
from datetime import datetime
import re
import os
import uuid
import time


class ClipOutliers(MariaMagic):
    """
    %clipoutliers [columns=col1,col2,...] [method=iqr|zscore]
                  [k=1.5] [z_thresh=3.0] [inplace=True|False]
                  [mode=preview|apply|rollback] [table=schema.table] [confirm=true|false]
                  [sample_size=100] [lock_timeout=10]

    Clamps (clips) extreme values to computed boundary limits.
    - method:
        iqr -> Tukey IQR method using k (default 1.5)
        zscore -> mean ± z_thresh * std (default z_thresh=3.0)
    - columns: comma-separated list of columns to operate on. If omitted, all numeric columns are used.
    - inplace: if True (default) modifies data["last_select"] in-place.
               if False stores clipped copy in data["last_select_clipped"].
    - mode:
        preview -> show what would happen (local + optional DB estimates)
        apply   -> perform clipping (local or DB)
        rollback-> restore DB backup created by apply
    Additionally, execution metadata is stored into a table `magic_metadata`.
    """

    def __init__(self, args=""):
        self.args = args

    def type(self):
        return "Line"

    def name(self):
        return "clipoutliers"

    def help(self):
        return (
            "%clipoutliers [columns=col1,col2,...] [method=iqr|zscore] [k=1.5] [z_thresh=3.0] [inplace=True|False]\n"
            "             [mode=preview|apply|rollback] [table=schema.table] [confirm=true|false]\n"
            "             [sample_size=100] [lock_timeout=10]\n"
            "Clamps extreme numeric values to computed boundaries (in-place by default).\n"
            "Execution metadata is recorded in table `magic_metadata`."
        )

    def _str_to_obj(self, s):
        """Convert strings like numbers or bools into Python objects."""
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
        """Parse key=value arguments."""
        if not input_str or input_str.strip() == "":
            return {}
        pairs = dict(token.split("=", 1) for token in shlex.split(input_str))
        for k, v in pairs.items():
            pairs[k] = self._str_to_obj(v)
        return pairs

    def _send_html(self, kernel, df):
        """Display DataFrame as HTML."""
        try:
            html = df.to_html(index=False)
            mime = "text/html"
        except Exception:
            html = str(df)
            mime = "text/plain"
        kernel.send_response(kernel.iopub_socket, "display_data",
                             {"data": {mime: html}, "metadata": {}})

    def _compute_bounds(self, series, method, k=1.5, z_thresh=3.0):
        """Compute (lower, upper) clipping bounds for a pandas Series."""
        s = series.dropna()
        if s.empty:
            return None, None
        if method == "iqr":
            q1 = s.quantile(0.25)
            q3 = s.quantile(0.75)
            iqr = q3 - q1
            lower = q1 - k * iqr
            upper = q3 + k * iqr
            return float(lower), float(upper)
        elif method == "zscore":
            mean = s.mean()
            std = s.std()
            if std == 0 or np.isnan(std):
                return None, None
            lower = mean - z_thresh * std
            upper = mean + z_thresh * std
            return float(lower), float(upper)
        else:
            raise ValueError(f"Unknown method {method}")

    # ---- New DB / metadata helpers ----
    def _sql_escape(self, val):
        """Escape a value for SQL single-quoted literal insert. None -> NULL"""
        if val is None:
            return "NULL"
        if not isinstance(val, str):
            val = str(val)
        # double single-quotes for SQL escaping
        return "'" + val.replace("'", "''") + "'"

    def _get_mariadb_client(self, kernel):
        """Return mariadb_client if present on kernel, else None"""
        return getattr(kernel, "mariadb_client", None)

    def _get_logger(self, kernel):
        """Return a logger on kernel if present, else create a temporary logger"""
        return getattr(kernel, "log", logging.getLogger(__name__))

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
                # fallthrough to manual approach
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
                df_list = pandas.read_html(result)
                if df_list and isinstance(df_list, list) and len(df_list) > 0:
                    val = df_list[0].iloc[0, 0]
                    if isinstance(val, float) and pandas.isna(val):
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

    def _ensure_metadata_table(self, kernel, db_name):
        """
        Create magic_metadata table if it doesn't exist.
        Includes rollback support columns (rollback_token, backup_table, original_table).
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
                log.error(f"Error creating magic_metadata table: {mariadb_client.run_statement('SHOW WARNINGS;')}")
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

    # DB helpers for threshold computation and parsing
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
                # try numeric conversion
                series = pd.to_numeric(df_list[0].iloc[:, 0], errors="coerce").dropna()
                if series.empty:
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

    # ---- End DB helpers ----

    def execute(self, kernel, data):
        """Execute the %clipoutliers magic with metadata logging and DB support."""
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

        # parse args
        columns_arg = args.get("columns", None)
        if isinstance(columns_arg, str):
            columns = [c.strip() for c in columns_arg.split(",") if c.strip()]
        elif isinstance(columns_arg, (list, tuple)):
            columns = list(columns_arg)
        else:
            columns = None

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
        inplace = bool(args.get("inplace", True))

        # mode and DB args
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

        # Determine numeric columns
        if columns is None:
            target_columns = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        else:
            missing_cols = [c for c in columns if c not in df.columns]
            if missing_cols:
                kernel._send_message("stderr", f"Column(s) not found: {', '.join(missing_cols)}")
                # log and return
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
            target_columns = [c for c in columns if pd.api.types.is_numeric_dtype(df[c])]
            non_numeric = [c for c in columns if c not in target_columns]
            if non_numeric:
                kernel._send_message("stdout", f"Warning: non-numeric columns skipped: {', '.join(non_numeric)}")

        if not target_columns:
            kernel._send_message("stderr", "No numeric target columns found to clip outliers.")
            # log and return
            try:
                self._ensure_metadata_table(kernel, db_name)
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns="",
                    operation_status="error",
                    message="No numeric target columns found to clip outliers.",
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
                messages = []
                total_would_change = 0
                combined_info = []
                for col in target_columns:
                    lower, upper = self._compute_bounds(df[col], method, k=k, z_thresh=z_thresh)
                    if lower is None and upper is None:
                        messages.append(f"Column '{col}': insufficient local data to compute bounds; skipped.")
                        combined_info.append((col, None, None, 0))
                        continue
                    mask = ((df[col] < lower) | (df[col] > upper)) & ~df[col].isna()
                    n_changed = int(mask.sum())
                    total_would_change += n_changed
                    messages.append(f"Column '{col}': would clip {n_changed} value(s) locally (bounds: {lower}, {upper}).")
                    combined_info.append((col, lower, upper, n_changed))

                n_before = len(df)
                n_after = n_before  # clipping doesn't remove rows locally
                kernel._send_message("stdout", f"PREVIEW (local): would modify {total_would_change} value(s) across {len(target_columns)} column(s).\n" + "\n".join(messages))

                # sample rows that have any out-of-bounds values
                mask_any = pd.Series(False, index=df.index)
                for col, lower, upper, _ in combined_info:
                    if lower is None and upper is None:
                        continue
                    mask_any = mask_any | (((df[col] < lower) | (df[col] > upper)) & ~df[col].isna())
                sample_rows = df[mask_any].head(sample_size).copy()
                if not sample_rows.empty:
                    # annotate which columns are OOB for each row
                    def oob_cols(r):
                        cols = [c for c, lower, upper, _ in combined_info if lower is not None and upper is not None and (pd.notna(r.get(c)) and (r.get(c) < lower or r.get(c) > upper))]
                        return ",".join(cols)
                    sample_rows["_oob_columns"] = sample_rows.apply(oob_cols, axis=1)

                    # ADD: compute and show clipped preview columns for visibility
                    for c, lower, upper, _ in combined_info:
                        clipped_col_name = f"{c}_clipped_preview"
                        try:
                            if lower is None and upper is None:
                                # no computed bounds; copy original values
                                sample_rows[clipped_col_name] = sample_rows[c]
                            else:
                                # use pandas clip to compute what the value would be after clipping
                                sample_rows[clipped_col_name] = sample_rows[c].clip(lower=lower, upper=upper)
                        except Exception:
                            # fallback: try elementwise clipping to avoid exceptions on mixed types
                            def _clip_val(v):
                                try:
                                    if pd.isna(v):
                                        return v
                                    if lower is not None and v < lower:
                                        return lower
                                    if upper is not None and v > upper:
                                        return upper
                                    return v
                                except Exception:
                                    return v
                            sample_rows[clipped_col_name] = sample_rows[c].apply(_clip_val)

                    try:
                        # prefer HTML display; this will include the *_clipped_preview columns
                        self._send_html(kernel, sample_rows)
                    except Exception:
                        kernel._send_message("stdout", str(sample_rows.head()))
                else:
                    kernel._send_message("stdout", "PREVIEW (local): no sample rows flagged as out-of-bounds.")

                # DB estimates if requested
                if table_full and mariadb_client is not None:
                    db_messages = []
                    predicates = []
                    for col in target_columns:
                        ok, thresholds, msg = self._compute_thresholds_db(mariadb_client, table_full, col, method, k=k, z_thresh=z_thresh, sample_size=sample_size)
                        if ok and thresholds:
                            lower = thresholds["lower"]
                            upper = thresholds["upper"]
                            predicates.append(f"({col} < {repr(lower)} OR {col} > {repr(upper)})")
                            db_messages.append(f"{col}: thresholds approx [{lower}, {upper}] ({msg})")
                        else:
                            db_messages.append(f"{col}: could not compute thresholds ({msg}) - skipped")

                    if predicates:
                        db_pred = " OR ".join(predicates)
                        try:
                            out = mariadb_client.run_statement(f"SELECT COUNT(*) FROM {table_full} WHERE {db_pred};")
                            cnt = self._parse_count_result(out)
                            if cnt is None:
                                kernel._send_message("stdout", "PREVIEW (db): could not parse count result (check permissions).")
                            else:
                                kernel._send_message("stdout", f"PREVIEW (db): estimated rows with OOB values: {cnt}.")
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
                lock_name = f"clipoutliers_rb_{token}"
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
                clip_map = {}
                messages = []

                # compute thresholds for each column using DB sampling
                for col in target_columns:
                    ok, thresholds, msg = self._compute_thresholds_db(mariadb_client, table_full, col, method, k=k, z_thresh=z_thresh, sample_size=sample_size)
                    if ok and thresholds:
                        clip_map[col] = (thresholds["lower"], thresholds["upper"])
                        messages.append(f"{col}: thresholds [{thresholds['lower']}, {thresholds['upper']}] ({msg})")
                    else:
                        messages.append(f"{col}: could not compute thresholds ({msg}); will leave column unchanged in DB apply")

                # Build SELECT exprs: for clipped cols use LEAST(GREATEST(col, lower), upper) AS col, else `col`
                select_exprs = []
                for c in df.columns:
                    if c in clip_map:
                        lower, upper = clip_map[c]
                        # use repr to preserve numeric literal format
                        select_exprs.append(f"LEAST(GREATEST({c}, {repr(lower)}), {repr(upper)}) AS {c}")
                    else:
                        select_exprs.append(c)
                select_sql = ", ".join(select_exprs)

                try:
                    lock_name = f"clipoutliers_apply_{token}"
                    got_lock = self._acquire_lock(mariadb_client, lock_name, timeout=lock_timeout)
                    if not got_lock:
                        kernel._send_message("stderr", "Could not acquire advisory lock; aborting apply.")
                        return

                    # create new table with clipped values
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
                    # log metadata (include token so user can rollback)
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns='\n'.join(target_columns),
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
                # Local in-place apply on data['last_select'] (existing behavior)
                target_df = df if inplace else df.copy(deep=True)
                messages = []
                total_clipped = 0
                operation_status = "success"
                try:
                    for col in target_columns:
                        series = target_df[col]
                        lower, upper = self._compute_bounds(series, method, k=k, z_thresh=z_thresh)
                        if lower is None and upper is None:
                            messages.append(f"Column '{col}': insufficient data to compute bounds; skipped.")
                            continue
                        mask = ((series < lower) | (series > upper)) & ~series.isna()
                        n_changed = int(mask.sum())
                        target_df[col] = series.clip(lower=lower, upper=upper)
                        total_clipped += n_changed
                        messages.append(f"Column '{col}': clipped {n_changed} value(s) (bounds: {lower:.4f}, {upper:.4f}).")
                    if inplace:
                        data["last_select"] = target_df
                        location_msg = "Modified in-place: data['last_select'] updated."
                    else:
                        data["last_select_clipped"] = target_df
                        location_msg = "Result stored in data['last_select_clipped'] (original unchanged)."
                    kernel._send_message("stdout", f"Clip outliers completed using {method}.\n"
                                                 + "\n".join(messages)
                                                 + f"\nTotal values clipped: {total_clipped}. {location_msg}")
                except Exception as e:
                    operation_status = "error"
                    messages.append(f"Fatal error during clipping: {e}")
                    kernel._send_message("stderr", f"Fatal error during clipping: {e}")

                # Insert metadata
                try:
                    args_for_db = self.args if isinstance(self.args, str) else str(self.args)
                    affected_columns_str = "\n".join(target_columns)
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
                except Exception as e:
                    try:
                        kernel._send_message("stdout", f"Warning: failed to write metadata: {e}")
                    except Exception:
                        pass

                # Show output (DataFrame)
                try:
                    self._send_html(kernel, target_df)
                except Exception:
                    pass

                return

        # fallback
        kernel._send_message("stderr", "Unknown execution path reached.")
        return
