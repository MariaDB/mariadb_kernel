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

from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score, confusion_matrix,
    mean_squared_error, mean_absolute_error, r2_score,
    roc_auc_score, classification_report
)
from sklearn.preprocessing import LabelEncoder

import matplotlib
# Use non-interactive backend if needed (safe in most notebook envs)
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import io
import base64

# Optional helper to reliably get current DB name (if available)
try:
    from mariadb_kernel.sql_fetch import SqlFetch
except Exception:
    SqlFetch = None


class EvaluateModel(MariaMagic):
    """
    %evaluate_model [model_name=last_model] [test_name=last_select_test] [pred_name=last_preds]
                    [problem=classification|regression]

    Nice, visual evaluation of a trained model: metrics card, confusion-matrix plot,
    classification report and a preview table of actual vs predicted.

    This version adds metadata logging to magic_metadata table:
      - creates magic_metadata if needed
      - logs error rows on failures and a success row on successful evaluation
    """
    def __init__(self, args=""):
        self.args = args

    def type(self):
        return "Line"

    def name(self):
        return "evaluate_model"

    def help(self):
        return "Evaluate a trained model on a test DataFrame and show metrics + predictions."

    # reuse helpers from previous version
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

    def _send_raw_html(self, kernel, html):
        """Send raw HTML to the frontend."""
        try:
            kernel.send_response(kernel.iopub_socket, "display_data",
                                 {"data": {"text/html": html}, "metadata": {}})
        except Exception:
            pass

    def _plot_confusion_matrix_to_datauri(self, cm, labels):
        """Draw confusion matrix (matplotlib) and return data URI PNG."""
        fig, ax = plt.subplots(figsize=(6, 5))
        im = ax.imshow(cm, interpolation='nearest')
        ax.set_title("Confusion matrix")
        ax.set_xlabel("Predicted")
        ax.set_ylabel("Actual")

        # Set tick labels
        ax.set_xticks(np.arange(len(labels)))
        ax.set_yticks(np.arange(len(labels)))
        ax.set_xticklabels(labels, rotation=45, ha="right")
        ax.set_yticklabels(labels)

        # Annotate cells
        thresh = cm.max() / 2.0 if cm.size else 0
        for i in range(cm.shape[0]):
            for j in range(cm.shape[1]):
                ax.text(j, i, format(int(cm[i, j]), 'd'),
                        ha="center", va="center",
                        fontsize=10)

        fig.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        data = base64.b64encode(buf.read()).decode("ascii")
        return f"data:image/png;base64,{data}"

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

        model_store_name = args.get("model_name", args.get("model", "last_model"))
        test_name = args.get("test_name", "last_select_test")
        pred_name = args.get("pred_name", "last_preds")
        problem_override = args.get("problem", None)

        # fetch model
        model = data.get(model_store_name)
        if model is None:
            msg = f"No model found in data['{model_store_name}']. Train and save a model first."
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

        # fetch test set
        test_df = data.get(test_name)
        if test_df is None or not isinstance(test_df, pd.DataFrame) or test_df.empty:
            msg = f"No test DataFrame found in data['{test_name}'] or it is empty."
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

        # infer problem if not provided
        if problem_override:
            problem = problem_override.lower()
        else:
            is_classifier = any(attr in dir(model) for attr in ("predict_proba", "decision_function", "classes_"))
            problem = "classification" if is_classifier else "regression"

        # get meta
        meta = data.get(model_store_name + "_meta", {}) or {}
        features = meta.get("features")
        target_col = meta.get("target") or meta.get("target_col")

        # fallback target inference
        if not target_col:
            if features:
                possible_targets = [c for c in test_df.columns if c not in features]
                if len(possible_targets) == 1:
                    target_col = possible_targets[0]

        if not target_col:
            msg = ("Target column not found in model meta and could not be inferred from test DataFrame. "
                   "Set data[model_name + '_meta']['target']='<target_column>' when training, or pass target info in meta.")
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
        if target_col not in test_df.columns:
            msg = f"Target column '{target_col}' not present in test DataFrame '{test_name}'."
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
        if not features:
            msg = "Model metadata does not contain 'features' list. Cannot build X_test."
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
        missing_features = [c for c in features if c not in test_df.columns]
        if missing_features:
            msg = f"Test DataFrame missing feature columns: {', '.join(missing_features)}"
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

        X_test = test_df[features].copy()
        y_true_orig = test_df[target_col].copy()

        # Predict
        try:
            preds_raw = model.predict(X_test)
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

        # predict_proba if available
        pred_proba = None
        if problem == "classification" and hasattr(model, "predict_proba"):
            try:
                proba = model.predict_proba(X_test)
                if proba.ndim == 2 and proba.shape[1] == 2:
                    pred_proba = proba[:, 1].tolist()
                else:
                    pred_proba = proba.tolist()
            except Exception:
                pred_proba = None

        # human-readable preds
        preds_display = preds_raw
        model_classes = getattr(model, "classes_", None)
        try:
            if model_classes is not None and pd.api.types.is_integer_dtype(np.asarray(preds_raw).dtype):
                preds_display = np.asarray(model_classes)[np.asarray(preds_raw).astype(int)]
        except Exception:
            preds_display = preds_raw

        # predictions DataFrame
        preds_df = test_df.copy(deep=True)
        preds_df["_predicted"] = preds_display
        if pred_proba is not None:
            preds_df["_pred_proba"] = pred_proba
        data[pred_name] = preds_df

        # metrics calculation
        out_lines = []
        metrics_html = ""
        cm_image_uri = None

        # We'll collect a compact metrics summary to store in metadata message on success
        metrics_summary = {}

        if problem == "classification":
            y_true_vals = np.asarray(y_true_orig)
            preds_vals = np.asarray(preds_display)

            def is_mixed(a, b):
                return (pd.api.types.is_numeric_dtype(a) and not pd.api.types.is_numeric_dtype(b)) or \
                       (pd.api.types.is_numeric_dtype(b) and not pd.api.types.is_numeric_dtype(a))

            if is_mixed(y_true_vals.dtype, preds_vals.dtype):
                y_metric = np.asarray(y_true_orig.astype(str))
                p_metric = np.asarray(pd.Series(preds_display).astype(str))
            else:
                if pd.api.types.is_numeric_dtype(y_true_vals) and pd.api.types.is_numeric_dtype(preds_vals):
                    y_metric = y_true_vals.astype(float)
                    p_metric = preds_vals.astype(float)
                else:
                    y_metric = np.asarray(y_true_orig.astype(str))
                    p_metric = np.asarray(pd.Series(preds_display).astype(str))

            try:
                acc = accuracy_score(y_metric, p_metric)
                prec = precision_score(y_metric, p_metric, average="weighted", zero_division=0)
                rec = recall_score(y_metric, p_metric, average="weighted", zero_division=0)
                f1 = f1_score(y_metric, p_metric, average="weighted", zero_division=0)
                cm = confusion_matrix(y_metric, p_metric)
            except Exception as e:
                msg = f"Error computing classification metrics: {e}"
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

            metrics_summary.update({"accuracy": float(acc), "precision": float(prec),
                                    "recall": float(rec), "f1": float(f1)})

            # ROC AUC if possible
            roc_text = "N/A"
            if pred_proba is not None and model_classes is not None:
                try:
                    class_to_idx = {str(c): i for i, c in enumerate(model_classes)}
                    y_idx = np.array([class_to_idx.get(str(v), None) for v in y_true_orig])
                    if None in y_idx:
                        roc_text = "Not computable: some test classes missing from model.classes_."
                    else:
                        proba_arr = np.asarray(pred_proba)
                        if proba_arr.ndim == 1:
                            roc_auc = roc_auc_score(y_idx.astype(int), proba_arr.astype(float))
                            roc_text = f"{roc_auc:.4f}"
                            metrics_summary["roc_auc"] = float(roc_auc)
                        else:
                            roc_auc = roc_auc_score(y_idx.astype(int), proba_arr, multi_class="ovr", average="weighted")
                            roc_text = f"{roc_auc:.4f}"
                            metrics_summary["roc_auc"] = float(roc_auc)
                except Exception:
                    roc_text = "Computation failed."

            # Prepare metrics HTML card
            metrics_html = f"""
            <div style="display:flex; gap:20px; align-items:flex-start; margin-bottom:10px;">
              <div style="border-radius:8px; padding:12px; box-shadow:0 1px 3px rgba(0,0,0,0.12);">
                <h4 style="margin:6px 0 8px 0;">Metrics</h4>
                <table style="border-collapse:collapse;">
                  <tr><td style="padding:4px 8px;"><strong>Accuracy</strong></td><td style="padding:4px 8px;">{acc:.4f}</td></tr>
                  <tr><td style="padding:4px 8px;"><strong>Precision (w)</strong></td><td style="padding:4px 8px;">{prec:.4f}</td></tr>
                  <tr><td style="padding:4px 8px;"><strong>Recall (w)</strong></td><td style="padding:4px 8px;">{rec:.4f}</td></tr>
                  <tr><td style="padding:4px 8px;"><strong>F1 (w)</strong></td><td style="padding:4px 8px;">{f1:.4f}</td></tr>
                  <tr><td style="padding:4px 8px;"><strong>ROC AUC</strong></td><td style="padding:4px 8px;">{roc_text}</td></tr>
                </table>
              </div>
            """

            # Render confusion matrix as image and embed
            try:
                if model_classes is not None:
                    label_names = [str(c) for c in model_classes]
                else:
                    # derive from the union of unique labels in y_metric and p_metric
                    uniq = sorted(set(np.unique(y_metric).tolist() + np.unique(p_metric).tolist()), key=lambda x: str(x))
                    label_names = [str(x) for x in uniq]
                cm_arr = np.asarray(cm, dtype=int)
                cm_image_uri = self._plot_confusion_matrix_to_datauri(cm_arr, label_names)
                metrics_html += f'<div style="border-radius:8px; padding:12px; box-shadow:0 1px 3px rgba(0,0,0,0.12);"><img src="{cm_image_uri}" alt="confusion matrix" style="max-width:100%; height:auto;"></div>'
            except Exception:
                metrics_html += '<div style="padding:8px;">Confusion matrix image failed to render.</div>'

            metrics_html += "</div>"  # close flex container

            # classification report text
            try:
                target_names = [str(c) for c in model_classes] if model_classes is not None else None
                report = classification_report(y_metric, p_metric, zero_division=0, target_names=target_names)
            except Exception:
                report = "Classification report not available."

        else:
            # regression branch
            try:
                preds_num = np.asarray(preds_raw).astype(float)
                y_true_num = np.asarray(y_true_orig).astype(float)
                rmse = float(np.sqrt(mean_squared_error(y_true_num, preds_num)))
                mae = float(mean_absolute_error(y_true_num, preds_num))
                r2 = float(r2_score(y_true_num, preds_num))
            except Exception as e:
                msg = f"Error computing regression metrics: {e}"
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

            metrics_summary.update({"rmse": float(rmse), "mae": float(mae), "r2": float(r2)})

            metrics_html = f"""
            <div style="border-radius:8px; padding:12px; box-shadow:0 1px 3px rgba(0,0,0,0.12);">
              <h4 style="margin:6px 0 8px 0;">Regression metrics</h4>
              <table style="border-collapse:collapse;">
                <tr><td style="padding:4px 8px;"><strong>RMSE</strong></td><td style="padding:4px 8px;">{rmse:.4f}</td></tr>
                <tr><td style="padding:4px 8px;"><strong>MAE</strong></td><td style="padding:4px 8px;">{mae:.4f}</td></tr>
                <tr><td style="padding:4px 8px;"><strong>R²</strong></td><td style="padding:4px 8px;">{r2:.4f}</td></tr>
              </table>
            </div>
            """
            report = None

        # Build final HTML to display (metrics + classification report text)
        html_parts = [
            f"<div style='font-family:Arial, sans-serif; margin:6px 0 12px 0;'>",
            metrics_html
        ]
        if problem == "classification":
            html_parts.append("<div style='margin-top:12px;'><h4>Classification report</h4>")
            html_parts.append(f"<pre style='white-space:pre-wrap; background:#f7f7f7; padding:8px; border-radius:6px;'>{report}</pre></div>")
            # also add textual confusion matrix below if image not present
            if cm_image_uri is None and 'cm' in locals():
                html_parts.append("<div style='margin-top:8px;'><h4>Confusion matrix</h4><pre>")
                html_parts.append(str(cm.tolist()))
                html_parts.append("</pre></div>")
        html_parts.append("</div>")

        # send HTML
        try:
            self._send_raw_html(kernel, "\n".join(html_parts))
        except Exception:
            pass

        # then show predictions table (actual vs predicted) using your helper
        try:
            # show a limited set (up to 200 rows)
            display_df = preds_df[[target_col, "_predicted"] + (["_pred_proba"] if "_pred_proba" in preds_df.columns else [])]
            self._send_html(kernel, display_df.head(200), title="Predictions preview (actual vs predicted)")
        except Exception:
            pass

        # Insert success metadata
        try:
            args_for_db = self.args if isinstance(self.args, str) else str(self.args)
            affected_cols_str = "\n".join(features)
            # craft a concise message describing main metrics
            if problem == "classification":
                main_metrics = ", ".join(f"{k}={v:.4f}" for k, v in metrics_summary.items())
            else:
                main_metrics = ", ".join(f"{k}={v:.4f}" for k, v in metrics_summary.items())
            message_str = f"Evaluation success. Model='{model_store_name}', test='{test_name}', preds_saved='{pred_name}'. {main_metrics}"
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
