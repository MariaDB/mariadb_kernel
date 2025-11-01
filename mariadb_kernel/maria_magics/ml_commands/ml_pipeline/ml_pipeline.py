# mlpipeline.py
# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.maria_magic import MariaMagic
import shlex
from distutils import util
import pandas as pd
import numpy as np
import json
import logging
import os
import re

# Import the other pipeline stages (paths kept as in your original snippet)
from mariadb_kernel.maria_magics.ml_commands.data_cleaning.missing import Missing
from mariadb_kernel.maria_magics.ml_commands.data_cleaning.dropmissing import DropMissing
from mariadb_kernel.maria_magics.ml_commands.data_cleaning.fillmissing import FillMissing
from mariadb_kernel.maria_magics.ml_commands.data_cleaning.outliers import Outliers
from mariadb_kernel.maria_magics.ml_commands.data_cleaning.dropoutliers import DropOutliers
from mariadb_kernel.maria_magics.ml_commands.data_cleaning.clipoutliers import ClipOutliers
from mariadb_kernel.maria_magics.ml_commands.data_preprocessing.encode import Encode
from mariadb_kernel.maria_magics.ml_commands.data_preprocessing.normalize import Normalize
from mariadb_kernel.maria_magics.ml_commands.data_preprocessing.standardize import Standardize
from mariadb_kernel.maria_magics.ml_commands.data_preprocessing.splitdata import SplitData  # placeholder safety
from mariadb_kernel.maria_magics.ml_commands.data_preprocessing.splitdata import SplitData
from mariadb_kernel.maria_magics.ml_commands.model_training.train_model import TrainModel
from mariadb_kernel.maria_magics.ml_commands.model_training.evaluate_model import EvaluateModel
from mariadb_kernel.maria_magics.ml_commands.model_training.savemodel import SaveModel
from mariadb_kernel.maria_magics.ml_commands.ml_pipeline.select_features import SelectFeatures
from mariadb_kernel.maria_magics.ml_commands.ml_pipeline.select_model import SelectModel

# Optional helper to reliably get current DB name (if available)
try:
    from mariadb_kernel.sql_fetch import SqlFetch
except Exception:
    SqlFetch = None


class MLPipeline(MariaMagic):
    """
    %mlpipeline target=target_col problem=classification|regression [features=col1,col2,...] [model=rf|auto]
                [save_path=/path/to/model.joblib]

    Automates an end-to-end ML pipeline on data['last_select'] with minimal input.
    """

    def __init__(self, args=""):
        self.args = args

    def type(self):
        return "Line"

    def name(self):
        return "mlpipeline"

    def help(self):
        return (
            "%mlpipeline target=target_col problem=classification|regression [features=col1,col2,...] [model=rf|auto]\n"
            "[save_path=/path/to/model.joblib]\n"
            "Automates an ML pipeline: cleaning, encoding, feature selection, preprocessing, model selection, training, and evaluation."
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
            try:
                return json.loads(s)
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

    def _send_html(self, kernel, df, title=None):
        try:
            html = df.to_html(index=False)
            if title:
                html = f"<h4>{title}</h4>" + html
            kernel.send_response(kernel.iopub_socket, "display_data",
                                {"data": {"text/html": html}, "metadata": {}})
        except Exception:
            pass

    def _send_message(self, kernel, channel, message):
        kernel._send_message(channel, f"[MLPipeline] {message}")

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

        df = data.get("last_select")
        if df is None or df.empty:
            msg = "No last_select found or DataFrame is empty."
            self._send_message(kernel, "stderr", msg)
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
            return False

        try:
            args = self.parse_args(self.args)
        except Exception as e:
            msg = f"Error parsing arguments: {e}. Use key=value syntax."
            self._send_message(kernel, "stderr", msg)
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
            return False

        # Parse arguments
        target = args.get("target")
        problem = args.get("problem")
        features_arg = args.get("features")
        model_name_arg = args.get("model", "auto")
        save_path = args.get("save_path", None)

        # Validate required arguments
        if not target:
            msg = "target argument is required (target=target_col)."
            self._send_message(kernel, "stderr", msg)
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
            return False
        if not problem:
            msg = "problem argument is required (problem=classification|regression)."
            self._send_message(kernel, "stderr", msg)
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
            return False
        if problem not in ("classification", "regression"):
            msg = "problem must be 'classification' or 'regression'."
            self._send_message(kernel, "stderr", msg)
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
            return False
        if target not in df.columns:
            msg = f"Target column '{target}' not found in DataFrame."
            self._send_message(kernel, "stderr", msg)
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
            return False

        # Parse features or set to all columns except target if not provided
        if features_arg:
            if isinstance(features_arg, str):
                features = [c.strip() for c in features_arg.split(",") if c.strip()]
            elif isinstance(features_arg, (list, tuple)):
                features = list(features_arg)
            else:
                msg = "features must be comma-separated string or list."
                self._send_message(kernel, "stderr", msg)
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
                return False
        else:
            features = [col for col in df.columns if col != target]
            if not features:
                msg = "No features available after excluding target column."
                self._send_message(kernel, "stderr", msg)
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
                return False

        # Validate features
        missing = [c for c in features if c not in df.columns]
        if missing:
            msg = f"Missing feature columns in DataFrame: {', '.join(missing)}"
            self._send_message(kernel, "stderr", msg)
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
            return False

        # Set defaults
        inplace = True
        missing_strategy = "drop"
        outlier_action = "none"
        encode_method = "onehot"
        scale_method = "standardize"
        test_size = 0.2
        val_size = 0.0
        stratify = target if problem == "classification" else None
        shuffle = True
        random_state = None
        model_store_name = "last_model"
        train_name = "last_select_train"
        test_name = "last_select_test"
        val_name = "last_select_val"
        feature_method = "correlation"
        k_features = 5
        primary_metric = "accuracy" if problem == "classification" else "r2"
        cv = 0

        # Work on a copy if not inplace
        working_df = df if inplace else df.copy(deep=True)
        data["last_select"] = working_df

        # Step 1: Handle missing values
        try:
            drop_args = f"columns={','.join(features + [target])}"
            DropMissing(drop_args).execute(kernel, data)
            cur_df = data.get("last_select")
            if cur_df is None or cur_df.empty:
                msg = "DataFrame is empty after dropping missing values."
                self._send_message(kernel, "stderr", msg)
                try:
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=drop_args,
                        affected_columns="\n".join(features),
                        operation_status="error",
                        message=msg,
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    pass
                return False
            # Refresh working_df reference after cleaning
            working_df = cur_df
        except Exception as e:
            msg = f"Error handling missing values: {e}"
            self._send_message(kernel, "stderr", msg)
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
            return False

        # Step 2: Encode categorical features
        try:
            # Recompute cat_columns on current working_df
            cat_columns = [c for c in features if c in working_df.columns and working_df[c].dtype in ["object", "category"]]
            if cat_columns:
                encode_args = f"method={encode_method} columns={','.join(cat_columns)} inplace=True drop_original=True mode=apply confirm=true"
                # reset any previous encoder
                data["last_select_encoder"] = None
                Encode(encode_args).execute(kernel, data)

                # after Encode runs, refresh working_df from shared data to see new columns
                working_df = data.get("last_select", working_df)

                if encode_method == "onehot":
                    encoder = data.get("last_select_encoder")
                    if not encoder:
                        msg = "Encoder not found after encoding. Ensure %encode saves the encoder to data['last_select_encoder']."
                        self._send_message(kernel, "stderr", msg)
                        try:
                            self._insert_metadata(
                                kernel=kernel,
                                command_name=self.name(),
                                arguments=encode_args,
                                affected_columns="\n".join(cat_columns),
                                operation_status="error",
                                message=msg,
                                db_name=db_name,
                                user_name=user_name
                            )
                        except Exception:
                            pass
                        return False
                    try:
                        # get_feature_names_out may require passing the original column names
                        try:
                            feature_names = list(encoder.get_feature_names_out(cat_columns))
                        except Exception:
                            # fallback for older sklearn or if encoder doesn't support that call
                            cats = getattr(encoder, "categories_", None)
                            feature_names = []
                            if cats is not None:
                                for cname, cat_list in zip(cat_columns, cats):
                                    for cat in cat_list:
                                        feature_names.append(f"{cname}_{str(cat)}")
                            else:
                                # As a last resort, build feature names from current working_df columns
                                # by selecting columns that start with the column name + "_"
                                feature_names = []
                                for cname in cat_columns:
                                    feature_names += [c for c in working_df.columns if c.startswith(cname + "_")]
                        # remove duplicates and ensure these features exist
                        feature_names = [str(fn) for fn in feature_names]
                        features = [c for c in features if c not in cat_columns] + feature_names
                    except Exception as e:
                        msg = f"Failed to retrieve encoded feature names: {e}"
                        self._send_message(kernel, "stderr", msg)
                        try:
                            self._insert_metadata(
                                kernel=kernel,
                                command_name=self.name(),
                                arguments=encode_args,
                                affected_columns="\n".join(cat_columns),
                                operation_status="error",
                                message=msg,
                                db_name=db_name,
                                user_name=user_name
                            )
                        except Exception:
                            pass
                        return False

                elif encode_method == "label":
                    # label encoding created columns <col>_lbl
                    features = [f"{c}_lbl" if c in cat_columns else c for c in features]

                elif encode_method == "ordinal":
                    # ordinal encoding created columns <col>_ord
                    features = [f"{c}_ord" if c in cat_columns else c for c in features]

                # Refresh working_df again (defensive)
                working_df = data.get("last_select", working_df)

                # Verify encoded features exist
                missing_encoded = [f for f in features if f not in working_df.columns]
                if missing_encoded:
                    # helpful debug output: list what columns do exist that are related
                    related_columns = []
                    for c in cat_columns:
                        related_columns += [col for col in working_df.columns if col.startswith(c + "_") or col.startswith(c + "_lbl") or col.startswith(c + "_ord")]
                    msg = f"Encoded features not found in DataFrame: {', '.join(missing_encoded)}"
                    self._send_message(kernel, "stderr", msg)
                    if related_columns:
                        self._send_message(kernel, "stderr", f"Available related columns: {', '.join(related_columns)}")
                    try:
                        self._insert_metadata(
                            kernel=kernel,
                            command_name=self.name(),
                            arguments=encode_args,
                            affected_columns="\n".join(missing_encoded),
                            operation_status="error",
                            message=msg,
                            db_name=db_name,
                            user_name=user_name
                        )
                    except Exception:
                        pass
                    return False
        except Exception as e:
            msg = f"Error during encoding: {e}"
            self._send_message(kernel, "stderr", msg)
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
            return False

        # Step 3: Feature selection (if features not provided)
        if not features_arg:
            try:
                select_features_args = f"target={target} method={feature_method} k={k_features} problem={problem} inplace={inplace}"
                SelectFeatures(select_features_args).execute(kernel, data)
                features = data.get("selected_features", [])
                if not features:
                    msg = "Feature selection failed to return features."
                    self._send_message(kernel, "stderr", msg)
                    try:
                        self._insert_metadata(
                            kernel=kernel,
                            command_name=self.name(),
                            arguments=select_features_args,
                            affected_columns="",
                            operation_status="error",
                            message=msg,
                            db_name=db_name,
                            user_name=user_name
                        )
                    except Exception:
                        pass
                    return False
                # Verify selected features exist
                working_df = data.get("last_select", working_df)
                missing_features = [f for f in features if f not in working_df.columns]
                if missing_features:
                    msg = f"Selected features not found in DataFrame: {', '.join(missing_features)}"
                    self._send_message(kernel, "stderr", msg)
                    try:
                        self._insert_metadata(
                            kernel=kernel,
                            command_name=self.name(),
                            arguments=select_features_args,
                            affected_columns="\n".join(missing_features),
                            operation_status="error",
                            message=msg,
                            db_name=db_name,
                            user_name=user_name
                        )
                    except Exception:
                        pass
                    return False
            except Exception as e:
                msg = f"Error during feature selection: {e}"
                self._send_message(kernel, "stderr", msg)
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
                return False

        # Step 4: Scale numeric features
        try:
            working_df = data.get("last_select", working_df)
            num_columns = [c for c in features if c in working_df.columns and pd.api.types.is_numeric_dtype(working_df[c])]
            if num_columns:
                scale_args = f"columns={','.join(num_columns)} inplace=True"
                Standardize(scale_args).execute(kernel, data)
        except Exception as e:
            msg = f"Error during scaling: {e}"
            self._send_message(kernel, "stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=scale_args if 'scale_args' in locals() else (self.args if isinstance(self.args, str) else str(self.args)),
                    affected_columns="\n".join(num_columns) if 'num_columns' in locals() else (",".join(features) if 'features' in locals() else ""),
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return False

        # Step 5: Split data
        try:
            split_args = f"test_size={test_size} val_size={val_size} shuffle={shuffle} " \
                        f"train_name={train_name} test_name={test_name} val_name={val_name} inplace={inplace}"
            if stratify:
                split_args += f" stratify={stratify}"
            if random_state is not None:
                split_args += f" random_state={random_state}"

            SplitData(split_args).execute(kernel, data)

            # Safely check that the split produced valid DataFrames
            train_df = data.get(train_name)
            test_df = data.get(test_name)

            if train_df is None or train_df.empty or test_df is None or test_df.empty:
                msg = "Data splitting failed to produce non-empty train/test sets."
                self._send_message(kernel, "stderr", msg)
                try:
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=split_args,
                        affected_columns="\n".join(features) if 'features' in locals() else "",
                        operation_status="error",
                        message=msg,
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    pass
                return False

        except Exception as e:
            msg = f"Error during data splitting: {e}"
            self._send_message(kernel, "stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=split_args if 'split_args' in locals() else (self.args if isinstance(self.args, str) else str(self.args)),
                    affected_columns="\n".join(features) if 'features' in locals() else "",
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return False

        # Step 6: Model selection or training
        try:
            # Treat 'auto' and None the same → use SelectModel
            if not model_name_arg or model_name_arg == "auto":
                select_model_args = (
                    f"features={','.join(features)} target={target} cv=5 "
                    f"primary_metric={primary_metric} problem={problem} output_name={model_store_name} inplace={inplace}"
                )
                SelectModel(select_model_args).execute(kernel, data)
                self._send_message(kernel, "stdout", "Automatically selected best model via SelectModel.")
            else:
                # Train a specific model
                train_args = (
                    f"model={model_name_arg} features={','.join(features)} target={target} "
                    f"model_name={model_store_name} test_name={test_name} cv={cv} inplace={inplace} problem={problem}"
                )
                TrainModel(train_args).execute(kernel, data)
                self._send_message(kernel, "stdout", f"Trained specified model '{model_name_arg}'.")

            # Validate model creation
            model_obj = data.get(model_store_name)
            if model_obj is None:
                msg = f"No model object created. Ensure SelectModel or TrainModel supports problem='{problem}'."
                self._send_message(kernel, "stderr", msg)
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
                return False

        except Exception as e:
            msg = f"Error during model training/selection: {e}"
            self._send_message(kernel, "stderr", msg)
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
            return False

        # Step 7: Evaluate model
        try:
            eval_args = f"model_name={model_store_name} test_name={test_name} problem={problem}"
            EvaluateModel(eval_args).execute(kernel, data)
        except Exception as e:
            msg = f"Error during model evaluation: {e}"
            self._send_message(kernel, "stderr", msg)
            try:
                self._insert_metadata(
                    kernel=kernel,
                    command_name=self.name(),
                    arguments=eval_args if 'eval_args' in locals() else (self.args if isinstance(self.args, str) else str(self.args)),
                    affected_columns="\n".join(features) if 'features' in locals() else "",
                    operation_status="error",
                    message=msg,
                    db_name=db_name,
                    user_name=user_name
                )
            except Exception:
                pass
            return False

        # Step 8: Save model if requested
        if save_path:
            try:
                # Ensure correct key for SaveModel command
                save_args = f"model_name_in_data={model_store_name} save_path={save_path}"
                SaveModel(save_args).execute(kernel, data)
                self._send_message(kernel, "stdout", f"Model saved to {save_path}.")
            except Exception as e:
                msg = f"Error saving model: {e}"
                self._send_message(kernel, "stderr", msg)
                try:
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=save_args if 'save_args' in locals() else (self.args if isinstance(self.args, str) else str(self.args)),
                        affected_columns=model_store_name,
                        operation_status="error",
                        message=msg,
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    pass
                return False
        else:
            msg = "You must provide save_path=/path/to/file.joblib"
            self._send_message(kernel, "stderr", msg)
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
            return False

        # Summary and success metadata
        success_msg = "ML pipeline completed successfully."
        self._send_message(kernel, "stdout", success_msg)
        try:
            args_for_db = self.args if isinstance(self.args, str) else str(self.args)
            affected_columns_str = "\n".join(features) if 'features' in locals() else ""
            message_str = f"{success_msg} model={model_store_name} saved_to={save_path}"
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
                self._send_message(kernel, "stdout", "Warning: failed to write metadata (continuing).")
            except Exception:
                pass

        return True
