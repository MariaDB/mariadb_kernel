# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.maria_magic import MariaMagic
import shlex
from distutils import util
import pandas as pd
import numpy as np
import joblib
import json
import logging
import os
import re

from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LogisticRegression, LinearRegression, Ridge, Lasso
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor, GradientBoostingClassifier, GradientBoostingRegressor, AdaBoostClassifier, AdaBoostRegressor
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier, KNeighborsRegressor
from sklearn.neural_network import MLPClassifier, MLPRegressor

# Optional external libraries
_XGBOOST_AVAILABLE = False
_LIGHTGBM_AVAILABLE = False
_CATBOOST_AVAILABLE = False
try:
    from xgboost import XGBClassifier, XGBRegressor
    _XGBOOST_AVAILABLE = True
except Exception:
    pass

try:
    from lightgbm import LGBMClassifier, LGBMRegressor
    _LIGHTGBM_AVAILABLE = True
except Exception:
    pass

try:
    from catboost import CatBoostClassifier, CatBoostRegressor
    _CATBOOST_AVAILABLE = True
except Exception:
    pass

# Optional helper to reliably get current DB name (if available)
try:
    from mariadb_kernel.sql_fetch import SqlFetch
except Exception:
    SqlFetch = None


class TrainModel(MariaMagic):
    """
    %train_model model=<name> features=col1,col2 target=target_col
                 [cv=0] [problem=classification|regression]
                 [model_name=last_model] [pred_name=last_preds] [test_name=last_select_test]
                 [save_path=/path/to/model.joblib] [inplace=True|False] [model_params={'n':1}]

    Train a model on data["last_select"] (TRAINING set). This magic DOES NOT perform
    splitting or scaling — run your preprocessing and %splitdata beforehand.

    This version adds metadata logging to magic_metadata table similar to SelectModel:
    - Ensures magic_metadata exists in current database
    - Inserts error/success rows for operations
    """
    def __init__(self, args=""):
        self.args = args

    def type(self):
        return "Line"

    def name(self):
        return "train_model"

    def help(self):
        return "Train a model on data['last_select'] (no split or scaling)."

    def _str_to_obj(self, s):
        # try int/float/bool, then JSON, then string unquote
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
        # try json
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

    def _choose_model(self, name, problem, params=None):
        p = params or {}
        name = name.lower()
        # Classification vs regression models where appropriate
        if name in ("logistic", "logistic_regression", "lr"):
            if problem != "classification":
                raise ValueError("LogisticRegression is for classification problems.")
            return LogisticRegression(max_iter=1000, **p)
        if name in ("rf", "random_forest"):
            return RandomForestClassifier(**p) if problem == "classification" else RandomForestRegressor(**p)
        if name in ("svc", "svm"):
            if problem != "classification":
                raise ValueError("SVC is for classification problems.")
            return SVC(probability=True, **p)
        if name in ("linear", "linear_regression"):
            if problem != "regression":
                raise ValueError("LinearRegression is for regression problems.")
            return LinearRegression(**p)
        if name == "ridge":
            if problem != "regression":
                raise ValueError("Ridge is for regression problems.")
            return Ridge(**p)
        if name == "lasso":
            if problem != "regression":
                raise ValueError("Lasso is for regression problems.")
            return Lasso(**p)
        if name == "knn":
            return KNeighborsClassifier(**p) if problem == "classification" else KNeighborsRegressor(**p)
        if name == "gbm":
            return GradientBoostingClassifier(**p) if problem == "classification" else GradientBoostingRegressor(**p)
        if name == "ada":
            return AdaBoostClassifier(**p) if problem == "classification" else AdaBoostRegressor(**p)
        if name == "mlp":
            return MLPClassifier(max_iter=1000, **p) if problem == "classification" else MLPRegressor(max_iter=1000, **p)
        if name == "xgboost":
            if not _XGBOOST_AVAILABLE:
                raise ImportError("xgboost not available in this environment.")
            return XGBClassifier(**p) if problem == "classification" else XGBRegressor(**p)
        if name == "lightgbm":
            if not _LIGHTGBM_AVAILABLE:
                raise ImportError("lightgbm not available in this environment.")
            return LGBMClassifier(**p) if problem == "classification" else LGBMRegressor(**p)
        if name == "catboost":
            if not _CATBOOST_AVAILABLE:
                raise ImportError("catboost not available in this environment.")
            # CatBoost often prints to stdout; keep default verbose False
            p = dict(p)
            p.setdefault("verbose", False)
            return CatBoostClassifier(**p) if problem == "classification" else CatBoostRegressor(**p)
        raise ValueError(f"Unknown model name '{name}'")

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

        # Load training DataFrame
        df = data.get("last_select")
        if df is None or df.empty:
            msg = "No last_select found or DataFrame is empty (training set required)."
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

        features_arg = args.get("features")
        target = args.get("target")
        model_name_arg = args.get("model", "rf")
        cv = int(args.get("cv", 0) or 0)
        problem_override = args.get("problem", None)
        test_name = args.get("test_name", "last_select_test")
        model_store_name = args.get("model_name", "last_model")
        inplace = bool(args.get("inplace", True))
        model_params = args.get("model_params", {}) or {}

        if not features_arg:
            msg = "features argument is required (features=col1,col2...)."
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
        if not target:
            msg = "target argument is required (target=target_col)."
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

        # parse features
        if isinstance(features_arg, str):
            features = [c.strip() for c in features_arg.split(",") if c.strip()]
        elif isinstance(features_arg, (list, tuple)):
            features = list(features_arg)
        else:
            msg = "features must be comma-separated string or list."
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

        missing = [c for c in features + [target] if c not in df.columns]
        if missing:
            msg = f"Missing columns in training DataFrame: {', '.join(missing)}"
            kernel._send_message("stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns="\n".join(features),
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return

        # Determine problem type
        if problem_override:
            problem = problem_override.lower()
            if problem not in ("classification", "regression"):
                msg = "problem must be 'classification' or 'regression'."
                kernel._send_message("stderr", msg)
                try:
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns="\n".join(features),
                        operation_status="error",
                        message=msg,
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    pass
                return
        else:
            # improved heuristic for problem detection
            tgt_ser = df[target]

            if pd.api.types.is_numeric_dtype(tgt_ser):
                nunique = int(tgt_ser.nunique(dropna=True))
                non_null_count = max(1, len(tgt_ser.dropna()))
                uniq_prop = nunique / non_null_count

                # treat as regression if:
                #  - float dtype, or
                #  - many distinct values (>20), or
                #  - distinct proportion high (e.g. >5% of rows)
                if pd.api.types.is_float_dtype(tgt_ser) or (nunique > 20) or (uniq_prop > 0.05):
                    problem = "regression"
                else:
                    # few distinct integer-like values -> classification (categorical target)
                    problem = "classification"
            else:
                problem = "classification"

        # Prepare X_train, y_train
        X_train = df[features].copy()
        y_train = df[target].copy()

        # NOTE: test set (if present) will be ignored in this modified flow — no predictions or metrics.
        # Keep reading test_df only to validate presence but do not use it.
        test_df = data.get(test_name)
        if isinstance(test_df, pd.DataFrame) and not test_df.empty:
            missing_test = [c for c in features + [target] if c not in test_df.columns]
            if missing_test:
                msg = f"Test DataFrame '{test_name}' missing columns: {', '.join(missing_test)}"
                kernel._send_message("stderr", msg)
                try:
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns="\n".join(features),
                        operation_status="error",
                        message=msg,
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    pass
                return

        # Instantiate model
        try:
            model = self._choose_model(model_name_arg, problem, params=model_params)
        except Exception as e:
            msg = f"Error creating model: {e}"
            kernel._send_message("stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns="\n".join(features),
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return

        # Cross-validation on training set if requested (kept)
        cv_results = None
        if cv and cv > 1:
            try:
                scoring = "accuracy" if problem == "classification" else "r2"
                cv_results = cross_val_score(model, X_train, y_train, cv=cv, scoring=scoring)
            except Exception as e:
                msg = f"Error during cross-validation: {e}"
                kernel._send_message("stderr", msg)
                try:
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns="\n".join(features),
                        operation_status="error",
                        message=msg,
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    pass
                return

        # Fit
        try:
            model.fit(X_train, y_train)
        except Exception as e:
            msg = f"Error fitting model: {e}"
            kernel._send_message("stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns="\n".join(features),
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return

        # Store only the trained model and minimal meta (no preds, no metrics, no joblib saving)
        try:
            data[model_store_name] = model

            # Save metadata including target so evaluate_model can find it
            meta = data.setdefault(model_store_name + "_meta", {})
            meta["problem"] = problem
            meta["features"] = features
            meta["target"] = target

            # If model exposes classes_, save them for easier decoding later
            if hasattr(model, "classes_"):
                try:
                    meta["classes"] = list(getattr(model, "classes_"))
                except Exception:
                    pass

        except Exception as e:
            msg = f"Error storing model: {e}"
            kernel._send_message("stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns="\n".join(features) if 'features' in locals() else "",
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return

        # Output concise summary
        out_lines = [f"Model '{model_name_arg}' trained and saved to data['{model_store_name}']. problem={problem}. train_rows={len(X_train)}"]
        if cv_results is not None:
            out_lines.append(f"cross-val (cv={cv}) scores: mean={float(np.mean(cv_results)):.4f}, std={float(np.std(cv_results)):.4f}")
        summary_msg = "\n".join(out_lines)
        kernel._send_message("stdout", summary_msg)

        # Insert success metadata (best-effort)
        try:
            args_for_db = self.args if isinstance(self.args, str) else str(self.args)
            affected_columns_str = "\n".join(features)
            message_str = summary_msg
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

        return
