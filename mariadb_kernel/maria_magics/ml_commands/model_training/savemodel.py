import joblib
import shlex
import json
import time
from distutils import util
import logging
from mariadb_kernel.maria_magics.maria_magic import MariaMagic
import os
import re
import pandas as pd

# Optional helper to reliably get current DB name (if available)
try:
    from mariadb_kernel.sql_fetch import SqlFetch
except Exception:
    SqlFetch = None


def _str_to_obj(s):
    try:
        return int(s)
    except Exception:
        pass
    try:
        return float(s)
    except Exception:
        pass
    try:
        return bool(util.strtobool(s))
    except Exception:
        pass
    try:
        return json.loads(s)
    except Exception:
        pass
    if isinstance(s, str) and len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
        return s[1:-1]
    return s


class SaveModel(MariaMagic):
    """
    %save_model model_name_in_data=last_model save_path=/tmp/model.joblib [overwrite=True|False]

    Saves a trained model (from the `data` dict) to a local file using joblib.

    This version:
      - If data contains model_key + "_meta", saves a dict {"model": model, "meta": meta}
      - Otherwise saves the raw model object (backwards compatible)
      - Writes metadata rows to magic_metadata (creates table if needed)
    """

    def __init__(self, args=""):
        self.args = args
        self.log = logging.getLogger(__name__)

    def type(self):
        return "Line"

    def name(self):
        return "save_model"

    def help(self):
        return "Save a trained model to a local .joblib file."

    def parse_args(self, input_str):
        if not input_str or input_str.strip() == "":
            return {}
        pairs = dict(token.split("=", 1) for token in shlex.split(input_str))
        for k, v in pairs.items():
            pairs[k] = _str_to_obj(v)
        return pairs

    # -------------------- small utilities for metadata --------------------
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
    # -------------------- end metadata helpers --------------------

    def execute(self, kernel, data):
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

        # parse args
        try:
            args = self.parse_args(self.args)
        except Exception:
            msg = "Error parsing arguments."
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

        model_key = args.get("model_name_in_data", "last_model")
        save_path = args.get("save_path")
        overwrite = bool(args.get("overwrite", False))

        if not save_path:
            msg = "You must provide save_path=/path/to/file.joblib"
            kernel._send_message("stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns=model_key or "",
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return

        model_obj = data.get(model_key)
        if model_obj is None:
            msg = f"No model found in data['{model_key}']."
            kernel._send_message("stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns=model_key,
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return

        # If file exists and overwrite=False
        if os.path.exists(save_path) and not overwrite:
            msg = f"File {save_path} already exists. Use overwrite=True to replace it."
            kernel._send_message("stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns=model_key,
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return

        # Try to include model metadata (if present) into the saved file so LoadModel can restore it
        meta = data.get(model_key + "_meta")

        try:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            if isinstance(meta, dict) and meta:
                # Save both model and meta in a single object for round-trip
                save_obj = {"model": model_obj, "meta": meta}
                joblib.dump(save_obj, save_path)
            else:
                # No meta available, save raw model (backwards compatible)
                joblib.dump(model_obj, save_path)

            success_msg = f"Model from data['{model_key}'] saved to {save_path}"
            kernel._send_message("stdout", success_msg)

            # Insert success metadata
            try:
                args_for_db = self.args if isinstance(self.args, str) else str(self.args)
                affected_columns_str = model_key
                # If meta exists, include a short description of key meta fields (features/target) in the message
                meta_info = ""
                try:
                    if isinstance(meta, dict):
                        feat = meta.get("features")
                        tgt = meta.get("target") or meta.get("target_col")
                        parts = []
                        if feat:
                            # limit length to avoid overly long field in DB
                            parts.append(f"features[{len(feat)}]")
                        if tgt:
                            parts.append(f"target={tgt}")
                        if parts:
                            meta_info = " (" + ", ".join(parts) + ")"
                except Exception:
                    meta_info = ""
                message_str = f"Saved model to {save_path}{meta_info}"
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=args_for_db,
                    affected_columns=affected_columns_str,
                    operation_status="success",
                    message=message_str,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                try:
                    kernel._send_message("stdout", "Warning: failed to write metadata (continuing).")
                except Exception:
                    pass

        except Exception as e:
            msg = f"Failed to save model: {e}"
            kernel._send_message("stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns=model_key,
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return
