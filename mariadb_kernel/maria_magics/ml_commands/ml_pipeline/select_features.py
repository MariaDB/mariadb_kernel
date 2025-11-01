# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.maria_magic import MariaMagic
import shlex
from distutils import util
import pandas as pd
import numpy as np
from sklearn.feature_selection import SelectKBest, f_classif, f_regression, RFE, mutual_info_classif, mutual_info_regression, chi2, VarianceThreshold
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LogisticRegression, Lasso
from sklearn.preprocessing import StandardScaler, MinMaxScaler
import logging
import os
import re

# Optional helper to reliably get current DB name (if available)
try:
    from mariadb_kernel.sql_fetch import SqlFetch
except Exception:
    SqlFetch = None


class SelectFeatures(MariaMagic):
    """
    %select_features target=target_col
                     [method=correlation|rf_importance|rfe|mutual_info|chi2|anova|l1_selection|variance]
                     [k=5] [problem=classification|regression]
                     [output_name=selected_features] [inplace=True|False]

    Identify the best features for training a model on data['last_select'].
    Uses all columns except the target column as features.

    Execution metadata is recorded in table `magic_metadata`.
    """

    def __init__(self, args=""):
        self.args = args

    def type(self):
        return "Line"

    def name(self):
        return "select_features"

    def help(self):
        return "Identify the best features for model training from data['last_select']."


    # -------------------- small utilities --------------------
    def _str_to_obj(self, s):
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
            import json
            return json.loads(s)
        except Exception:
            pass
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

        # Load training DataFrame
        df = data.get("last_select")
        if df is None or df.empty:
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

        target = args.get("target")
        method = args.get("method", "correlation").lower()
        k = args.get("k", 5)
        problem_override = args.get("problem", None)
        output_name = args.get("output_name", "selected_features")
        inplace = bool(args.get("inplace", True))

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

        if target not in df.columns:
            msg = f"Target column '{target}' not found in DataFrame."
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

        # Use all columns except the target as features
        features = [col for col in df.columns if col != target]
        if not features:
            msg = "No features available after excluding target column."
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
                        affected_columns="",
                        operation_status="error",
                        message=msg,
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    pass
                return
        else:
            tgt_ser = df[target]
            if pd.api.types.is_numeric_dtype(tgt_ser):
                nunique = int(tgt_ser.nunique(dropna=True))
                non_null_count = max(1, len(tgt_ser.dropna()))
                uniq_prop = nunique / non_null_count
                if pd.api.types.is_float_dtype(tgt_ser) or nunique > 20 or uniq_prop > 0.05:
                    problem = "regression"
                else:
                    problem = "classification"
            else:
                problem = "classification"

        # Prepare data
        X = df[features].copy()
        y = df[target].copy()

        # Handle missing values (simple imputation for feature selection)
        try:
            if problem == "regression":
                X = X.fillna(X.mean(numeric_only=True))
            else:
                X = X.fillna(X.mode().iloc[0])
        except Exception:
            msg = "Features contain non-numeric data or unhandled missing values."
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

        if X.isna().any().any():
            msg = "Features contain non-numeric data or unhandled missing values."
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

        # Scale data for methods that require it
        if method in ("chi2", "l1_selection"):
            scaler = MinMaxScaler() if method == "chi2" else StandardScaler()
            try:
                X = pd.DataFrame(scaler.fit_transform(X), columns=X.columns, index=X.index)
            except Exception as e:
                msg = f"Error scaling data: {e}"
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

        # Feature selection
        try:
            if method == "correlation":
                correlations = X.corrwith(y, method="pearson").abs()
                scores = correlations.sort_values(ascending=False)
                selected_features = scores.head(k).index.tolist()
                result_df = pd.DataFrame({
                    "Feature": scores.index,
                    "Score": scores.values
                })

            elif method == "rf_importance":
                model = RandomForestClassifier() if problem == "classification" else RandomForestRegressor()
                model.fit(X, y)
                importances = pd.Series(model.feature_importances_, index=features)
                scores = importances.sort_values(ascending=False)
                selected_features = scores.head(k).index.tolist()
                result_df = pd.DataFrame({
                    "Feature": scores.index,
                    "Score": scores.values
                })

            elif method == "rfe":
                estimator = RandomForestClassifier() if problem == "classification" else RandomForestRegressor()
                selector = RFE(estimator, n_features_to_select=k)
                selector.fit(X, y)
                ranking = pd.Series(selector.ranking_, index=features)
                scores = 1 / (ranking + 1)
                selected_features = ranking[ranking == 1].index.tolist()
                result_df = pd.DataFrame({
                    "Feature": ranking.index,
                    "Score": scores,
                    "Ranking": ranking
                }).sort_values("Score", ascending=False)

            elif method == "mutual_info":
                score_func = mutual_info_classif if problem == "classification" else mutual_info_regression
                selector = SelectKBest(score_func=score_func, k=k)
                selector.fit(X, y)
                scores = pd.Series(selector.scores_, index=features)
                scores = scores.sort_values(ascending=False)
                selected_features = scores.head(k).index.tolist()
                result_df = pd.DataFrame({
                    "Feature": scores.index,
                    "Score": scores.values
                })

            elif method == "chi2":
                if problem != "classification":
                    msg = "chi2 method is only for classification problems."
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
                if (X < 0).any().any():
                    msg = "chi2 requires non-negative features."
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
                selector = SelectKBest(score_func=chi2, k=k)
                selector.fit(X, y)
                scores = pd.Series(selector.scores_, index=features)
                scores = scores.sort_values(ascending=False)
                selected_features = scores.head(k).index.tolist()
                result_df = pd.DataFrame({
                    "Feature": scores.index,
                    "Score": scores.values
                })

            elif method == "anova":
                score_func = f_classif if problem == "classification" else f_regression
                selector = SelectKBest(score_func=score_func, k=k)
                selector.fit(X, y)
                scores = pd.Series(selector.scores_, index=features)
                scores = scores.sort_values(ascending=False)
                selected_features = scores.head(k).index.tolist()
                result_df = pd.DataFrame({
                    "Feature": scores.index,
                    "Score": scores.values
                })

            elif method == "l1_selection":
                model = LogisticRegression(penalty="l1", solver="liblinear", max_iter=1000) if problem == "classification" else Lasso(alpha=0.01)
                model.fit(X, y)
                scores = pd.Series(np.abs(model.coef_.ravel() if problem == "classification" else model.coef_), index=features)
                scores = scores.sort_values(ascending=False)
                selected_features = scores[scores > 0].head(k).index.tolist()
                result_df = pd.DataFrame({
                    "Feature": scores.index,
                    "Score": scores.values
                })

            elif method == "variance":
                selector = VarianceThreshold(threshold=0.0)
                selector.fit(X)
                variances = pd.Series(selector.variances_, index=features)
                scores = variances.sort_values(ascending=False)
                selected_features = scores.head(k).index.tolist()
                result_df = pd.DataFrame({
                    "Feature": scores.index,
                    "Score": scores.values
                })

            else:
                msg = "method must be one of 'correlation', 'rf_importance', 'rfe', 'mutual_info', 'chi2', 'anova', 'l1_selection', or 'variance'."
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

        except Exception as e:
            msg = f"Error during feature selection: {e}"
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

        # Store results in data dict
        try:
            data[output_name] = selected_features
            data[output_name + "_meta"] = {
                "method": method,
                "problem": problem,
                "target": target,
                "k": k,
                "all_scores": result_df.to_dict()
            }
        except Exception as e:
            msg = f"Error storing results: {e}"
            kernel._send_message("stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=self.args if isinstance(self.args, str) else str(self.args),
                    affected_columns="\n".join(selected_features) if 'selected_features' in locals() else "",
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return

        # Display results
        try:
            self._send_html(kernel, result_df, title=f"Feature Selection Results (method={method})")
        except Exception:
            pass

        success_msg = f"Selected {len(selected_features)} features saved to data['{output_name}']: {', '.join(selected_features)}"
        kernel._send_message("stdout", success_msg)

        # Insert metadata (best-effort)
        try:
            args_for_db = self.args if isinstance(self.args, str) else str(self.args)
            affected_columns_str = "\n".join(selected_features)
            message_str = success_msg
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
