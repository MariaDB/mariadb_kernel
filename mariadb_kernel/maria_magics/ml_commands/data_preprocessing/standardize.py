# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.maria_magic import MariaMagic
import shlex
from distutils import util
import pandas as pd
from sklearn.preprocessing import StandardScaler
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


class Standardize(MariaMagic):
    """
    %standardize [columns=col1,col2,...] [inplace=True|False]
                 [mode=preview|apply|rollback] [table=schema.table]
                 [confirm=true|false] [sample_size=100] [lock_timeout=10]

    Standardizes numeric columns using sklearn's StandardScaler
    (zero mean and unit variance).

    - columns: comma-separated list of columns to standardize.
               If omitted, all numeric columns are used.
    - inplace: if True (default), modifies data["last_select"] in-place.
               if False, stores result in data["last_select_standardized"].
    - mode: preview/apply/rollback (preview default).
      * preview: show local preview and optional DB stats if table=... provided.
      * apply: local in-place (default) or DB versioned apply when table=... and confirm=true.
      * rollback: restore a previously-created backup (needs mariadb_client).
    """
    def __init__(self, args=""):
        self.args = args

    def type(self):
        return "Line"

    def name(self):
        return "standardize"

    def help(self):
        return (
            "%standardize [columns=col1,col2,...] [inplace=True|False]\n"
            "            [mode=preview|apply|rollback] [table=schema.table] [confirm=true]\n"
            "            [sample_size=100] [lock_timeout=10]\n"
            "Standardizes numeric columns using sklearn's StandardScaler."
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

    def _send_html(self, kernel, df):
        try:
            html = df.to_html(index=False)
            kernel.send_response(kernel.iopub_socket, "display_data",
                                 {"data": {"text/html": html}, "metadata": {}})
        except Exception:
            pass

    # -------------------- metadata / DB helpers (best-effort) --------------------
    def _get_mariadb_client(self, kernel):
        return getattr(kernel, "mariadb_client", None)

    def _get_logger(self, kernel):
        return getattr(kernel, "log", logging.getLogger(__name__))

    def _sql_escape(self, val):
        """Escape a value for SQL single-quoted literal insert. None -> NULL"""
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
        # include rollback columns so apply/rollback can record/locate backups
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

    def _parse_two_value_result(self, res):
        """
        Parse results expected to have two values (e.g., AVG and STD).
        Returns (val1, val2) or (None, None) if parsing fails.
        """
        if not res:
            return None, None
        try:
            dfs = pd.read_html(res)
            if dfs and len(dfs) > 0:
                r = dfs[0].iloc[0]
                v0 = None
                v1 = None
                try:
                    v0 = float(r.iloc[0]) if pd.notna(r.iloc[0]) else None
                except Exception:
                    v0 = None
                try:
                    v1 = float(r.iloc[1]) if r.size > 1 and pd.notna(r.iloc[1]) else None
                except Exception:
                    v1 = None
                return v0, v1
        except Exception:
            vals = re.findall(r"<td.*?>(.*?)</td>", str(res), flags=re.S | re.I)
            if vals:
                def tofloat(txt):
                    txt = re.sub(r"<.*?>", "", txt).strip()
                    if txt.lower() == "null" or txt == "":
                        return None
                    try:
                        return float(txt)
                    except Exception:
                        return None
                v0 = tofloat(vals[0])
                v1 = tofloat(vals[1]) if len(vals) > 1 else None
                return v0, v1
        # fallback: try whitespace split
        try:
            parts = [p for p in str(res).split() if p.strip()]
            if len(parts) >= 2:
                try:
                    return float(parts[0]), float(parts[1])
                except Exception:
                    return None, None
        except Exception:
            pass
        return None, None

    # -------------------- end metadata helpers --------------------

    def execute(self, kernel, data):
        df = data.get("last_select")
        # Prepare metadata context early
        db_name = self._get_db_name(kernel)
        user_name = self._get_user_name(kernel)
        try:
            self._ensure_metadata_table(kernel, db_name)
        except Exception:
            try:
                kernel._send_message("stdout", "Warning: failed to ensure metadata table (continuing).")
            except Exception:
                pass

        if df is None or (hasattr(df, "empty") and df.empty):
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

        columns_arg = args.get("columns", None)
        if isinstance(columns_arg, str):
            columns = [c.strip() for c in columns_arg.split(",") if c.strip()]
        elif isinstance(columns_arg, (list, tuple)):
            columns = list(columns_arg)
        else:
            columns = None

        inplace = bool(args.get("inplace", True))
        mode = str(args.get("mode", "preview")).lower()
        mode = mode if mode in {"preview", "apply", "rollback"} else "preview"
        table_full = args.get("table", None)
        confirm = bool(args.get("confirm", False))
        sample_size = int(args.get("sample_size", 100))
        lock_timeout = int(args.get("lock_timeout", 10))

        mariadb_client = self._get_mariadb_client(kernel)
        log = self._get_logger(kernel)

        # Determine target columns (numeric)
        target_df = df if inplace else df.copy(deep=True)
        if columns is None:
            target_columns = [c for c in target_df.columns if pd.api.types.is_numeric_dtype(target_df[c])]
        else:
            missing_cols = [c for c in columns if c not in target_df.columns]
            if missing_cols:
                msg = f"Missing columns: {', '.join(missing_cols)}"
                kernel._send_message("stderr", msg)
                try:
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
            target_columns = columns

        if not target_columns:
            msg = "No numeric columns to standardize."
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

        # ---------------- PREVIEW ----------------
        if mode == "preview":
            try:
                messages = []
                # local preview: means/std and sample transformed
                local_stats = {}
                for col in target_columns:
                    s = pd.to_numeric(df[col], errors="coerce").dropna()
                    if s.empty:
                        messages.append(f"Local: Column '{col}' has no numeric non-null values; skipped.")
                        local_stats[col] = (None, None)
                        continue
                    mean = float(s.mean())
                    std = float(s.std(ddof=0))  # population std to match DB STDDEV_POP
                    local_stats[col] = (mean, std)
                    messages.append(f"Local: Column '{col}': mean={mean}, std={std}")

                kernel._send_message("stdout", "PREVIEW (local):\n" + "\n".join(messages))

                # show sample transformed rows
                try:
                    sample = df[target_columns].head(sample_size).copy()
                    for col in target_columns:
                        mean, std = local_stats.get(col, (None, None))
                        if mean is None or std is None or std == 0:
                            # cannot standardize sensibly; show original
                            sample[col + "_std_preview"] = sample[col]
                        else:
                            sample[col + "_std_preview"] = (pd.to_numeric(sample[col], errors="coerce") - mean) / std
                    if not sample.empty:
                        self._send_html(kernel, sample.head(20))
                except Exception:
                    pass

                # DB preview if requested
                if table_full and mariadb_client is not None:
                    db_msgs = []
                    for col in target_columns:
                        try:
                            # use AVG and STDDEV_POP for stable population std
                            out = mariadb_client.run_statement(f"SELECT AVG({col}), STDDEV_POP({col}) FROM {table_full};")
                            if mariadb_client.iserror():
                                db_msgs.append(f"DB: Column '{col}': AVG/STD query failed (permissions?).")
                                continue
                            mean_db, std_db = self._parse_two_value_result(out)
                            db_msgs.append(f"DB: Column '{col}': mean={mean_db}, std={std_db}")
                            if mean_db is None or std_db is None:
                                db_msgs.append(f"DB: Column '{col}': cannot compute mean/std (NULL).")
                                continue
                            if std_db == 0:
                                expr = f"CASE WHEN {col} IS NULL THEN NULL ELSE 0 END AS {col}"
                            else:
                                expr = f"CASE WHEN {col} IS NULL THEN NULL ELSE (({col} - {repr(mean_db)}) / {repr(std_db)}) END AS {col}"
                            db_msgs.append(f"DB: Column '{col}' expression: {expr}")
                        except Exception as e:
                            db_msgs.append(f"DB: Column '{col}' AVG/STD query exception: {e}")
                    kernel._send_message("stdout", "PREVIEW (db):\n" + "\n".join(db_msgs))

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

        # ---------------- ROLLBACK ----------------
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

                lock_name = f"standardize_rb_{token}"
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
                        # try to infer original table name from arguments in metadata
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

        # ---------------- APPLY ----------------
        if mode == "apply":
            # DB-target apply if table provided and mariadb_client present
            if table_full and mariadb_client is not None:
                if not confirm:
                    kernel._send_message("stderr", "DB apply requires confirm=true to proceed. Preview first, then re-run with confirm=true.")
                    return

                token = str(uuid.uuid4()).replace('-', '')[:16]
                backup_table = f"{table_full}_backup_{token}"
                new_table = f"{table_full}_vnew_{token}"

                # collect mean/std from DB per column
                col_stats = {}
                msgs = []
                for col in target_columns:
                    try:
                        out = mariadb_client.run_statement(f"SELECT AVG({col}), STDDEV_POP({col}) FROM {table_full};")
                        if mariadb_client.iserror():
                            msgs.append(f"{col}: AVG/STD query failed.")
                            col_stats[col] = (None, None)
                            continue
                        mean_db, std_db = self._parse_two_value_result(out)
                        col_stats[col] = (mean_db, std_db)
                        msgs.append(f"{col}: mean={mean_db}, std={std_db}")
                    except Exception as e:
                        col_stats[col] = (None, None)
                        msgs.append(f"{col}: exception computing stats: {e}")

                # build select expressions (preserve non-target columns)
                select_exprs = []
                for c in df.columns:
                    if c in target_columns:
                        mean_db, std_db = col_stats.get(c, (None, None))
                        if mean_db is None or std_db is None:
                            # cannot compute, keep original as-is
                            select_exprs.append(c)
                        elif std_db == 0:
                            # constant zero (or map to 0)
                            select_exprs.append(f"CASE WHEN {c} IS NULL THEN NULL ELSE 0 END AS {c}")
                        else:
                            select_exprs.append(f"CASE WHEN {c} IS NULL THEN NULL ELSE (({c} - {repr(mean_db)}) / {repr(std_db)}) END AS {c}")
                    else:
                        select_exprs.append(c)

                select_sql = ", ".join(select_exprs)

                try:
                    lock_name = f"standardize_apply_{token}"
                    got_lock = self._acquire_lock(mariadb_client, lock_name, timeout=lock_timeout)
                    if not got_lock:
                        kernel._send_message("stderr", "Could not acquire advisory lock; aborting apply.")
                        return

                    # create new table CTAS
                    mariadb_client.run_statement(f"CREATE TABLE {new_table} AS SELECT {select_sql} FROM {table_full};")
                    if mariadb_client.iserror():
                        kernel._send_message("stderr", "Failed to create new table for apply (CTAS failed).")
                        return

                    # atomic rename: original -> backup, new -> original
                    mariadb_client.run_statement(f"RENAME TABLE {table_full} TO {backup_table}, {new_table} TO {table_full};")
                    if mariadb_client.iserror():
                        kernel._send_message("stderr", "RENAME TABLE failed (apply may be inconsistent).")
                        return

                    kernel._send_message("stdout", f"Apply completed: original preserved as {backup_table}.")
                    # log metadata with rollback token
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns='\n'.join(target_columns),
                        operation_status='applied',
                        message=f'applied_backup={backup_table};details={"|".join(msgs)}',
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
                # local apply (existing behavior)
                operation_status = "success"
                messages = []
                try:
                    scaler = StandardScaler()
                    target_df[target_columns] = scaler.fit_transform(target_df[target_columns])
                    summary_msg = f"Standardized {len(target_columns)} column(s) (mean=0, std=1)."
                    messages.append(summary_msg)
                    if inplace:
                        data["last_select"] = target_df
                        location_msg = "Updated data['last_select'] in-place."
                        kernel._send_message("stdout", f"{summary_msg} {location_msg}")
                    else:
                        data["last_select_standardized"] = target_df
                        location_msg = "Stored in data['last_select_standardized']."
                        kernel._send_message("stdout", f"{summary_msg} {location_msg}")
                except Exception as e:
                    operation_status = "error"
                    err_msg = f"Error during standardization: {e}"
                    kernel._send_message("stderr", err_msg)
                    messages.append(err_msg)

                # show
                try:
                    self._send_html(kernel, target_df)
                except Exception:
                    pass

                # metadata
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
                except Exception:
                    try:
                        kernel._send_message("stdout", "Warning: failed to write metadata (continuing).")
                    except Exception:
                        pass

                return

        # fallback
        kernel._send_message("stderr", "Unknown execution path reached.")
        return
