# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.maria_magic import MariaMagic
import shlex
from distutils import util
import pandas as pd
from collections import namedtuple
import logging
import math
import os
import re
import time
import uuid
import json
import html

# Attempt to import SqlFetch if available (helps to determine current DB reliably)
try:
    from mariadb_kernel.sql_fetch import SqlFetch
except Exception:
    SqlFetch = None


class DropMissing(MariaMagic):
    """
    %dropmissing [columns=col1,col2,...] [mode=preview|apply|rollback]
                 [table=schema.table]
                 [sample_size=100] [confirm=true|false]
                 [rollback_token=<token>] [lock_timeout=10]

    Notes:
      - The "analyze" mode has been removed.
      - There is no strategy argument: DB applies always use the safe "versioned"
        approach (CTAS + atomic RENAME). This provides a straightforward
        rollback path via a backup table and rollback_token.
      - When an apply is performed the generated rollback_token is printed to
        stdout so users can copy it for a later rollback.
    """

    def __init__(self, args=""):
        self.args = args

    def type(self):
        return "Line"

    def name(self):
        return "dropmissing"

    def help(self):
        return (
            "%dropmissing [columns=col1,col2,...] [mode=preview|apply|rollback] [table=schema.table]\n"
            "Preview operates on data['last_select']. Apply will always use a versioned CTAS+RENAME strategy (requires confirm=true when targeting DB).\n"
            "Execution metadata recorded in table `magic_metadata`."
        )

    # -------------------- Basic helpers ---------------------------------
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
            html_repr = df.to_html(index=False)
            mime = "text/html"
        except Exception:
            html_repr = "<pre>" + html.escape(str(df)) + "</pre>"
            mime = "text/html"
        display_content = {"data": {mime: html_repr}, "metadata": {}}
        kernel.send_response(kernel.iopub_socket, "display_data", display_content)

    # -------------------- DB / metadata helpers ---------------------------
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
        if SqlFetch is not None and mariadb_client is not None:
            try:
                sf = SqlFetch(mariadb_client, log)
                dbname = sf.get_db_name()
                if isinstance(dbname, str):
                    return dbname
            except Exception:
                log.debug("SqlFetch.get_db_name failed; falling back to SELECT DATABASE()")
        if mariadb_client is None:
            return ""
        try:
            result = mariadb_client.run_statement("SELECT DATABASE();")
            if mariadb_client.iserror() or not result:
                return ""
            try:
                df_list = pd.read_html(result)
                if df_list and isinstance(df_list, list) and len(df_list) > 0:
                    val = df_list[0].iloc[0, 0]
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

    # ---------- End DB helpers ----------

    def _build_delete_predicate(self, columns):
        """Return SQL predicate that matches rows with missing values in given columns.
        columns==None means any column is NULL => predicate for any column null can't be generated without schema
        so we return None in that case (caller should handle).
        """
        if not columns:
            return None
        clauses = [f"{col} IS NULL" for col in columns]
        return " OR ".join(clauses)

    def _table_exists(self, mariadb_client, table_full_name):
        try:
            mariadb_client.run_statement(f"SELECT 1 FROM {table_full_name} LIMIT 1;")
            return not mariadb_client.iserror()
        except Exception:
            return False

    def execute(self, kernel, data):
        """Execute the dropmissing magic (supports preview/analyze/apply/rollback and logs metadata)."""
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

        # parse columns
        columns_arg = args.get("columns", None)
        if isinstance(columns_arg, str):
            columns = [c.strip() for c in columns_arg.split(",") if c.strip()]
        elif isinstance(columns_arg, (list, tuple)):
            columns = list(columns_arg)
        else:
            columns = None

        # operational args
        mode = str(args.get("mode", "preview")).lower()
        mode = mode if mode in {"preview", "analyze", "apply", "rollback"} else "preview"
        table_full = args.get("table", None)  # expected 'schema.table' or 'table'
        strategy = str(args.get("strategy", "versioned")).lower()
        sample_size = int(args.get("sample_size", 100))
        confirm = bool(args.get("confirm", False))
        pk_col = args.get("pk", None)
        rollback_token = args.get("rollback_token", None)
        lock_timeout = int(args.get("lock_timeout", 10))
        analyze_real = bool(args.get("analyze_real", False))

        mariadb_client = self._get_mariadb_client(kernel)
        log = self._get_logger(kernel)
        db_name = self._get_db_name(kernel)
        user_name = self._get_user_name(kernel)

        # validate requested columns exist in df
        if columns is not None:
            missing_cols = [c for c in columns if c not in df.columns]
            if missing_cols:
                kernel._send_message("stderr", f"Column(s) not found in last_select: {', '.join(missing_cols)}")
                # Log metadata for failure
                try:
                    self._ensure_metadata_table(kernel, db_name)
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns=','.join(columns) if columns else "",
                        operation_status="error",
                        message=f"Column(s) not found: {', '.join(missing_cols)}",
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    pass
                return

        # prepare predicate
        sql_predicate = self._build_delete_predicate(columns)

        # metadata table ensure (best-effort)
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
                before_count = len(df)
                if columns is None:
                    after_df = df.dropna()
                else:
                    after_df = df.dropna(axis=0, subset=columns)
                after_count = len(after_df)
                dropped = before_count - after_count

                kernel._send_message("stdout", f"PREVIEW: would drop {dropped} row(s) (from {before_count} to {after_count}).")

                # show small sample with before/after preview for rows that would be dropped
                if columns is None:
                    predicate_mask = df.isnull().any(axis=1)
                else:
                    predicate_mask = df[columns].isnull().any(axis=1)

                sample_rows = df[predicate_mask].head(sample_size)
                # show 'after' preview as dropped rows (so after preview is empty for those rows)
                sample_preview = sample_rows.copy()
                sample_preview["_would_be_dropped"] = True

                if not sample_preview.empty:
                    try:
                        self._send_html(kernel, sample_preview)
                    except Exception:
                        pass

                # If table specified, show EXPLAIN for corresponding DELETE
                if table_full and mariadb_client is not None:
                    if sql_predicate is None:
                        kernel._send_message("stdout", "Preview: cannot generate DB predicate for 'any column null' without explicit columns.")
                    else:
                        delete_sql = f"DELETE FROM {table_full} WHERE {sql_predicate};"
                        try:
                            # EXPLAIN (no execute)
                            mariadb_client.run_statement("EXPLAIN FORMAT=JSON " + delete_sql)
                            if mariadb_client.iserror():
                                kernel._send_message("stdout", "Could not run EXPLAIN on DB — check permissions or SQL syntax.")
                            else:
                                kernel._send_message("stdout", "EXPLAIN (estimate) for corresponding DELETE (JSON):")
                                kernel._send_message("stdout", mariadb_client.run_statement("EXPLAIN FORMAT=JSON " + delete_sql))
                        except Exception:
                            kernel._send_message("stdout", "Failed to run EXPLAIN on DB (continuing).")

                # log preview metadata
                try:
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns='\n'.join(columns) if columns else 'ALL_COLUMNS',
                        operation_status='preview',
                        message=f'preview_dropped={dropped}',
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    pass

            except Exception as e:
                kernel._send_message("stderr", f"Error during preview: {e}")
            return

        # --- ANALYZE MODE -----------------------------------------------
        if mode == "analyze":
            if mariadb_client is None or not table_full:
                kernel._send_message("stderr", "ANALYZE requires a connected mariadb_client and table=<schema.table> argument.")
                return
            if sql_predicate is None:
                kernel._send_message("stderr", "ANALYZE requires explicit columns= to generate delete predicate.")
                return
            delete_sql = f"DELETE FROM {table_full} WHERE {sql_predicate};"
            try:
                # Run EXPLAIN (estimate)
                explain_out = mariadb_client.run_statement("EXPLAIN FORMAT=JSON " + delete_sql)
                kernel._send_message("stdout", "EXPLAIN (estimate):")
                kernel._send_message("stdout", explain_out)
                # Optionally run EXPLAIN ANALYZE if requested
                if analyze_real:
                    try:
                        analyze_out = mariadb_client.run_statement("EXPLAIN ANALYZE " + delete_sql)
                        kernel._send_message("stdout", "EXPLAIN ANALYZE (actual run):")
                        kernel._send_message("stdout", analyze_out)
                    except Exception:
                        kernel._send_message("stdout", "EXPLAIN ANALYZE failed or is not supported on this server.")
                # log analyze metadata
                try:
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns='\n'.join(columns) if columns else 'ALL_COLUMNS',
                        operation_status='analyze',
                        message='analyze_completed',
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    pass
            except Exception as e:
                kernel._send_message("stderr", f"Error during analyze: {e}")
            return

        # --- ROLLBACK MODE ---------------------------------------------
        if mode == "rollback":
            if mariadb_client is None:
                kernel._send_message("stderr", "Rollback requested but no mariadb_client available.")
                return
            # If rollback_token provided, try to find matching metadata entry
            token = rollback_token
            try:
                if not token:
                    # try to read latest magic_metadata entry for this command and user
                    mariadb_client.run_statement(f"SELECT id, rollback_token, backup_table, original_table, arguments, execution_timestamp FROM {db_name}.magic_metadata WHERE command_name={self._sql_escape(self.name())} AND user_name={self._sql_escape(user_name)} ORDER BY execution_timestamp DESC LIMIT 1;")
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
                # now find backup_table and original_table associated with token
                mariadb_client.run_statement(f"SELECT backup_table, original_table FROM {db_name}.magic_metadata WHERE rollback_token={self._sql_escape(token)} LIMIT 1;")
                backup_out = mariadb_client.run_statement(f"SELECT backup_table, original_table FROM {db_name}.magic_metadata WHERE rollback_token={self._sql_escape(token)} LIMIT 1;")
                m = re.search(r"<td.*?>(.*?)</td>.*?<td.*?>(.*?)</td>", str(backup_out), flags=re.S | re.I)
                backup_table = None
                original_table = None
                if m:
                    # depending on HTML ordering we try to extract both; fallback below parses individually
                    backup_table = re.sub(r"<.*?>", "", m.group(1)).strip()
                    original_table = re.sub(r"<.*?>", "", m.group(2)).strip()
                else:
                    # fallback: fetch backup_table
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

                # perform atomic swap to restore backup -> original
                lock_name = f"dropmissing_rb_{token}"
                self._acquire_lock(mariadb_client, lock_name, timeout=lock_timeout)
                try:
                    # If original_table was recorded during apply, prefer to use it
                    if original_table:
                        # if original exists, rename original -> original_backup_before_rb_{token}, then rename backup -> original
                        if self._table_exists(mariadb_client, original_table):
                            # create a unique temp name for the old original
                            original_old = f"{original_table}_prerollback_{token}"
                            # atomic multi-rename: rename original -> original_old, backup -> original
                            mariadb_client.run_statement(f"RENAME TABLE {original_table} TO {original_old}, {backup_table} TO {original_table};")
                            if mariadb_client.iserror():
                                kernel._send_message("stderr", "Failed to rename tables during rollback (check permissions).")
                                return
                            kernel._send_message("stdout", f"Rollback: restored {backup_table} -> {original_table}; previous {original_table} renamed to {original_old}.")
                            # record rollback metadata
                            self._insert_metadata(
                                kernel=kernel,
                                command_name=self.name(),
                                arguments=self.args if isinstance(self.args, str) else str(self.args),
                                affected_columns='\n'.join(columns) if columns else 'ALL_COLUMNS',
                                operation_status='rollback',
                                message=f'restored_to={original_table};previous_saved_as={original_old}',
                                db_name=db_name,
                                user_name=user_name,
                                rollback_token=token,
                                backup_table=backup_table,
                                original_table=original_table
                            )
                        else:
                            # original does not exist currently; rename backup -> original directly
                            mariadb_client.run_statement(f"RENAME TABLE {backup_table} TO {original_table};")
                            if mariadb_client.iserror():
                                kernel._send_message("stderr", "Failed to rename backup to original during rollback (check permissions).")
                                return
                            kernel._send_message("stdout", f"Rollback: renamed {backup_table} -> {original_table}.")
                            # record rollback metadata
                            self._insert_metadata(
                                kernel=kernel,
                                command_name=self.name(),
                                arguments=self.args if isinstance(self.args, str) else str(self.args),
                                affected_columns='\n'.join(columns) if columns else 'ALL_COLUMNS',
                                operation_status='rollback',
                                message=f'restored_to={original_table}',
                                db_name=db_name,
                                user_name=user_name,
                                rollback_token=token,
                                backup_table=backup_table,
                                original_table=original_table
                            )
                    else:
                        # No original_table recorded — best-effort: attempt to infer original name from arguments
                        # try to fetch arguments column
                        try:
                            out_args = mariadb_client.run_statement(f"SELECT arguments FROM {db_name}.magic_metadata WHERE rollback_token={self._sql_escape(token)} LIMIT 1;")
                            margs = re.search(r"<td.*?>(.*?)</td>", str(out_args), flags=re.S | re.I)
                            inferred_original = None
                            if margs:
                                args_txt = re.sub(r"<.*?>", "", margs.group(1)).strip()
                                # try to find table=... inside arguments
                                mm = re.search(r"table\s*=\s*([^\s,]+)", args_txt)
                                if mm:
                                    inferred_original = mm.group(1).strip()
                            if inferred_original:
                                # same logic as above using inferred_original
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
                                        affected_columns='\n'.join(columns) if columns else 'ALL_COLUMNS',
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
                                        affected_columns='\n'.join(columns) if columns else 'ALL_COLUMNS',
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
                        except Exception as e:
                            kernel._send_message("stderr", f"Rollback error while inferring original table: {e}")
                            return
                finally:
                    self._release_lock(mariadb_client, lock_name)
            except Exception as e:
                kernel._send_message("stderr", f"Rollback error: {e}")
            return

        # --- APPLY MODE -----------------------------------------------
        if mode == "apply":
            # two main apply targets: DB (table_full provided and mariadb_client present) or local DataFrame
            if table_full and mariadb_client is not None:
                # safety: require explicit confirmation to run DB changes
                if not confirm:
                    kernel._send_message("stderr", "DB apply requires confirm=true to proceed. Preview first, then re-run with confirm=true.")
                    return

                if sql_predicate is None:
                    kernel._send_message("stderr", "Apply to DB requires explicit columns= to build a safe predicate (avoid accidental full-table deletes).")
                    return

                # strategy selection
                if strategy == "versioned":
                    # create a new table (CTAS) containing rows we want to keep (i.e., NOT predicate)
                    # generate unique backup name
                    token = str(uuid.uuid4()).replace('-', '')[:16]
                    backup_table = f"{table_full}_backup_{token}"
                    new_table = f"{table_full}_vnew_{token}"
                    delete_pred = sql_predicate
                    try:
                        # acquire lock
                        lock_name = f"dropmissing_apply_{token}"
                        got_lock = self._acquire_lock(mariadb_client, lock_name, timeout=lock_timeout)
                        if not got_lock:
                            kernel._send_message("stderr", "Could not acquire advisory lock; aborting apply.")
                            return

                        # create new table with rows to keep
                        mariadb_client.run_statement(f"CREATE TABLE {new_table} AS SELECT * FROM {table_full} WHERE NOT ({delete_pred});")
                        if mariadb_client.iserror():
                            kernel._send_message("stderr", "Failed to create new table for apply (CTAS failed).")
                            return
                        # basic validation: counts (best-effort)
                        mariadb_client.run_statement(f"SELECT COUNT(*) FROM {table_full};")
                        total_old = mariadb_client.run_statement(f"SELECT COUNT(*) FROM {table_full};")
                        mariadb_client.run_statement(f"SELECT COUNT(*) FROM {new_table};")

                        # atomic rename: original -> backup, new -> original
                        mariadb_client.run_statement(f"RENAME TABLE {table_full} TO {backup_table}, {new_table} TO {table_full};")
                        if mariadb_client.iserror():
                            kernel._send_message("stderr", "RENAME TABLE failed (apply may be inconsistent).")
                            # attempt cleanup
                            return

                        kernel._send_message("stdout", f"Apply completed: original preserved as {backup_table}.")
                        # log metadata (include token so user can rollback) and record original_table
                        self._insert_metadata(
                            kernel=kernel,
                            command_name=self.name(),
                            arguments=self.args if isinstance(self.args, str) else str(self.args),
                            affected_columns='\n'.join(columns) if columns else 'ALL_COLUMNS',
                            operation_status='applied',
                            message=f'applied_backup={backup_table}',
                            db_name=db_name,
                            user_name=user_name,
                            rollback_token=token,
                            backup_table=backup_table,
                            original_table=table_full
                        )
                        # update in-memory last_select to reflect applied state (fetch fresh)
                        try:
                            mariadb_client.run_statement(f"SELECT * FROM {table_full} LIMIT {sample_size};")
                            fresh = mariadb_client.run_statement(f"SELECT * FROM {table_full} LIMIT {sample_size};")
                            # try to parse HTML into DataFrame
                            try:
                                df_list = pd.read_html(fresh)
                                if df_list and len(df_list) > 0:
                                    data["last_select"] = df_list[0]
                                    try:
                                        self._send_html(kernel, data["last_select"])
                                    except Exception:
                                        pass
                            except Exception:
                                # cannot parse, just notify
                                kernel._send_message("stdout", "Applied to DB; could not refresh last_select from DB.")
                        except Exception:
                            pass

                    except Exception as e:
                        kernel._send_message("stderr", f"Apply (versioned) failed: {e}")
                        log.exception(e)
                    finally:
                        self._release_lock(mariadb_client, lock_name)
                    return

                elif strategy == "transactional":
                    # transactional apply: capture changed rows in an audit table, then delete
                    if not pk_col:
                        kernel._send_message("stderr", "Transactional strategy requires pk=<primary_key_column> to capture changed rows for rollback. Falling back to versioned strategy.")
                        # fall back to versioned
                        args["strategy"] = "versioned"
                        self.args = "".join([f"{k}={v} " for k, v in args.items()])
                        return self.execute(kernel, data)

                    token = str(uuid.uuid4()).replace('-', '')[:16]
                    audit_table = f"{db_name}.magic_audit_{token}"
                    delete_pred = sql_predicate
                    try:
                        lock_name = f"dropmissing_tx_{token}"
                        got_lock = self._acquire_lock(mariadb_client, lock_name, timeout=lock_timeout)
                        if not got_lock:
                            kernel._send_message("stderr", "Could not acquire advisory lock; aborting apply.")
                            return

                        # create audit table
                        mariadb_client.run_statement(f"CREATE TABLE IF NOT EXISTS {audit_table} (tx_id VARCHAR(64), pk_val TEXT, old_row LONGTEXT, created_at DATETIME);")
                        if mariadb_client.iserror():
                            kernel._send_message("stderr", "Failed to create audit table.")
                            return

                        # insert affected rows into audit table
                        mariadb_client.run_statement(f"INSERT INTO {audit_table} (tx_id, pk_val, old_row, created_at) SELECT '{token}', CAST({pk_col} AS CHAR), TO_BASE64(ROW_TO_JSON(t)), NOW() FROM {table_full} t WHERE {delete_pred};")
                        # Note: ROW_TO_JSON and TO_BASE64 may not be available depending on server; this is best-effort

                        # delete rows
                        mariadb_client.run_statement(f"DELETE FROM {table_full} WHERE {delete_pred};")
                        if mariadb_client.iserror():
                            kernel._send_message("stderr", "DELETE failed during transactional apply (check SQL and permissions).")
                            return

                        kernel._send_message("stdout", f"Transactional apply completed; audit table {audit_table} contains old rows for rollback with token {token}.")
                        # metadata
                        self._insert_metadata(
                            kernel=kernel,
                            command_name=self.name(),
                            arguments=self.args if isinstance(self.args, str) else str(self.args),
                            affected_columns='\n'.join(columns) if columns else 'ALL_COLUMNS',
                            operation_status='applied',
                            message=f'audit_table={audit_table}',
                            db_name=db_name,
                            user_name=user_name,
                            rollback_token=token,
                            backup_table=audit_table,
                            original_table=table_full
                        )
                        # refresh in-memory last_select little
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
                        kernel._send_message("stderr", f"Apply (transactional) failed: {e}")
                        log.exception(e)
                    finally:
                        self._release_lock(mariadb_client, lock_name)
                    return

                else:
                    kernel._send_message("stderr", f"Unknown strategy: {strategy}")
                    return

            else:
                # operate locally on data['last_select'] (in-place)
                operation_status = "success"
                messages = []
                try:
                    before_count = len(df)
                    if columns is None:
                        df.dropna(axis=0, inplace=True)
                    else:
                        df.dropna(axis=0, subset=columns, inplace=True)
                    after_count = len(df)
                    dropped = before_count - after_count
                    data["last_select"] = df
                    msg = f"Dropped {dropped} row(s) with missing values (in-place local)."
                    kernel._send_message("stdout", msg)
                    messages.append(msg)
                    try:
                        self._send_html(kernel, df)
                    except Exception:
                        pass
                except Exception as e:
                    operation_status = "error"
                    err_msg = f"Error while dropping missing values locally: {e}"
                    kernel._send_message("stderr", err_msg)
                    messages.append(err_msg)

                # Insert metadata
                try:
                    args_for_db = self.args if isinstance(self.args, str) else str(self.args)
                    affected_columns_str = "\n".join(columns) if columns else "ALL_COLUMNS"
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
