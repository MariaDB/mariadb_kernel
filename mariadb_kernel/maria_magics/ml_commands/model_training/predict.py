# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.maria_magic import MariaMagic
import pandas as pd
import numpy as np
import shlex
import json
from distutils import util
import logging
import os
import re

# Optional helper to reliably get current DB name (if available)
try:
    from mariadb_kernel.sql_fetch import SqlFetch
except Exception:
    SqlFetch = None


class Predict(MariaMagic):
    """
    %predict_model model_name=last_model data_name=last_select_test output_name=last_preds
                   [show_cols=10] [proba=True|False]

    You can also provide inline values:
      %predict_model model_name=last_model data_name=[38, 80000.0] output_name=last_preds

    This version records metadata into magic_metadata table (creates it if needed),
    logging errors and a final success entry on completion.
    """

    def __init__(self, args=""):
        self.args = args
        self.log = logging.getLogger(__name__)

    def type(self):
        return "Line"

    def name(self):
        return "predict_model"

    def help(self):
        return "Run predictions using a trained model stored in data[model_name], with optional inline feature values."

    def _str_to_obj(self, s):
        # try to interpret numbers, booleans, lists, or JSON
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
        # strip quotes
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

    # -------------------- metadata helpers (copied/adapted) --------------------
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

        model_name = args.get("model_name", "last_model")
        data_arg = args.get("data_name", "last_select_test")
        output_name = args.get("output_name", "last_preds")
        show_cols = int(args.get("show_cols", 10))
        show_proba = bool(args.get("proba", False))

        # --- 1. Retrieve model ---
        model = data.get(model_name)
        if model is None:
            msg = f"No model found in data['{model_name}']. Train one first."
            kernel._send_message("stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns=model_name or "",
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return

        # --- 2. Load metadata ---
        meta = data.get(model_name + "_meta", {})
        features = meta.get("features")
        problem = meta.get("problem", "regression")

        if not features:
            kernel._send_message("stderr", "Model meta missing 'features'. Using numeric columns only if applicable.")
            # leave features as empty list so we can attempt to infer columns from df later
            features = []

        # --- 3. Determine input mode ---
        df = None
        inline_used = False
        inline_vals = None
        if isinstance(data_arg, list):
            # Inline list of feature values
            inline_used = True
            inline_vals = data_arg
            if not features:
                msg = "Cannot use inline values: model has no stored feature names."
                kernel._send_message("stderr", msg)
                try:
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns=",".join(features) if features else "",
                        operation_status="error",
                        message=msg,
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    pass
                return
            if len(data_arg) != len(features):
                msg = f"Number of values ({len(data_arg)}) doesn't match expected features ({len(features)}): {features}"
                kernel._send_message("stderr", msg)
                try:
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns=",".join(features),
                        operation_status="error",
                        message=msg,
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    pass
                return
            df = pd.DataFrame([data_arg], columns=features)
            kernel._send_message("stdout", f"Using inline feature values for prediction: {dict(zip(features, data_arg))}")

        elif isinstance(data_arg, str) and data_arg.startswith("[") and data_arg.endswith("]"):
            # If user passed JSON array as string, parse it
            try:
                vals = json.loads(data_arg)
                inline_used = True
                inline_vals = vals
                if not features:
                    msg = "Cannot use inline values: model has no stored feature names."
                    kernel._send_message("stderr", msg)
                    try:
                        self._insert_metadata(
                            kernel=kernel,
                            command_name=self.name(),
                            arguments=self.args if isinstance(self.args, str) else str(self.args),
                            affected_columns=",".join(features) if features else "",
                            operation_status="error",
                            message=msg,
                            db_name=db_name,
                            user_name=user_name
                        )
                    except Exception:
                        pass
                    return
                if len(vals) != len(features):
                    msg = f"Number of values ({len(vals)}) doesn't match expected features ({len(features)}): {features}"
                    kernel._send_message("stderr", msg)
                    try:
                        self._insert_metadata(
                            kernel=kernel,
                            command_name=self.name(),
                            arguments=self.args if isinstance(self.args, str) else str(self.args),
                            affected_columns=",".join(features),
                            operation_status="error",
                            message=msg,
                            db_name=db_name,
                            user_name=user_name
                        )
                    except Exception:
                        pass
                    return
                df = pd.DataFrame([vals], columns=features)
                kernel._send_message("stdout", f"Using inline feature values for prediction: {dict(zip(features, vals))}")
            except Exception as e:
                msg = f"Error parsing inline data list: {e}"
                kernel._send_message("stderr", msg)
                try:
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns=",".join(features) if features else "",
                        operation_status="error",
                        message=msg,
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    pass
                return
        else:
            # DataFrame-based mode
            df = data.get(data_arg)
            if df is None or (isinstance(df, pd.DataFrame) and df.empty):
                msg = f"No DataFrame found in data['{data_arg}'] or it's empty."
                kernel._send_message("stderr", msg)
                try:
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns=",".join(features) if features else data_arg,
                        operation_status="error",
                        message=msg,
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    pass
                return
            if not isinstance(df, pd.DataFrame):
                msg = f"data['{data_arg}'] is not a pandas DataFrame."
                kernel._send_message("stderr", msg)
                try:
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns=data_arg,
                        operation_status="error",
                        message=msg,
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    pass
                return

        # --- 4. Align columns to features ---
        df_cols = df.columns.tolist()
        missing = [c for c in features if c not in df_cols]
        extra = [c for c in df_cols if c not in features]

        if missing:
            # we fill missing with zeros (behavior from original)
            kernel._send_message("stderr", f"Missing columns not in input: {missing}. Filling with zeros.")
        if extra:
            kernel._send_message("stderr", f"Ignoring extra columns not seen during training: {extra}.")

        # If no features are known, attempt to use numeric columns from df
        if not features:
            # prefer numeric columns
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            if numeric_cols:
                features = numeric_cols
                kernel._send_message("stdout", f"Inferred features from numeric columns: {features}")
            else:
                msg = "No features available and could not infer numeric columns for prediction."
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

        X = pd.DataFrame({col: df[col] if col in df.columns else 0 for col in features})

        # --- 5. Run predictions ---
        try:
            if show_proba and problem == "classification" and hasattr(model, "predict_proba"):
                preds = model.predict_proba(X)
                if hasattr(model, "classes_"):
                    class_labels = [str(c) for c in model.classes_]
                    pred_df = pd.DataFrame(preds, columns=[f"proba_{c}" for c in class_labels])
                else:
                    pred_df = pd.DataFrame(preds, columns=[f"proba_{i}" for i in range(preds.shape[1])])
            else:
                y_pred = model.predict(X)
                pred_df = pd.DataFrame(y_pred, columns=["prediction"])
        except Exception as e:
            msg = f"Error during prediction: {e}"
            kernel._send_message("stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns=",".join(features),
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return

        # --- 6. Save & display ---
        data[output_name] = pred_df

        try:
            # prefer html display if kernel supports it
            try:
                self._send_html(kernel, pred_df.head(show_cols), title=f"Predictions ({output_name})")
            except Exception:
                kernel._send_message("stdout", pred_df.head(show_cols).to_string(index=False))
        except Exception:
            pass

        success_msg = f"Predictions stored in data['{output_name}'] with shape={pred_df.shape}"
        kernel._send_message("stdout", success_msg)

        # Insert success metadata
        try:
            args_for_db = self.args if isinstance(self.args, str) else str(self.args)
            affected_cols_str = "\n".join(features) if features else ""
            # include a short summary mentioning whether inline values were used
            inline_part = f" inline_values={inline_vals}" if inline_used else ""
            message_str = f"Prediction success. model={model_name}, data_arg={data_arg}, output={output_name}, shape={pred_df.shape}{inline_part}"
            self._insert_metadata(
                kernel=kernel,
                command_name=self.name(),
                arguments=args_for_db,
                affected_columns=affected_cols_str,
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

        return
