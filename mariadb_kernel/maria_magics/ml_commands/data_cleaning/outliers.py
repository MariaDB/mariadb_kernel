# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.maria_magic import MariaMagic
import shlex
from distutils import util
import pandas as pd
import numpy as np
import io
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import logging
import os
import re

# Optional helper to reliably get current DB name (if available)
try:
    from mariadb_kernel.sql_fetch import SqlFetch
except Exception:
    SqlFetch = None


class Outliers(MariaMagic):
    """
    %outliers [columns=col1,col2,...] [method=iqr|zscore] [k=1.5] [z_thresh=3.0] [plot=True|False]

    Detects outliers (NON IN-PLACE) and stores a copy of the DataFrame with boolean
    indicator columns in data['last_select_outliers'].

    Additionally logs execution metadata into `magic_metadata` table:
      id, command_name, arguments, execution_timestamp, affected_columns,
      operation_status, message, db_name, user_name
    """

    def __init__(self, args=""):
        self.args = args

    def type(self):
        return "Line"

    def name(self):
        return "outliers"

    def help(self):
        return (
            "%outliers [columns=col1,col2,...] [method=iqr|zscore] [k=1.5] [z_thresh=3.0] [plot=True|False]\n"
            "Detects outliers in data['last_select'] (non in-place). Results placed in data['last_select_outliers']."
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
        if not input_str or input_str.strip() == "":
            return {}
        pairs = dict(token.split("=", 1) for token in shlex.split(input_str))
        for k, v in pairs.items():
            pairs[k] = self._str_to_obj(v)
        return pairs

    def _send_html(self, kernel, df):
        try:
            html = df.to_html(index=False)
            mime = "text/html"
        except Exception:
            html = str(df)
            mime = "text/plain"
        display_content = {"data": {mime: html}, "metadata": {}}
        kernel.send_response(kernel.iopub_socket, "display_data", display_content)

    def _send_image(self, kernel, fig):
        buf = io.BytesIO()
        try:
            fig.tight_layout()
        except Exception:
            pass
        fig.savefig(buf, format="png", bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        img_bytes = buf.read()
        display_content = {"data": {"image/png": img_bytes}, "metadata": {}}
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

    def _build_plots(self, df_numeric, outlier_masks):
        """
        Builds a figure with:
          - top: boxplot of df_numeric (showfliers=False) with overlay of detected outliers
          - bottom: scatter plot (index vs value) for each column; outliers highlighted in red
        """
        cols = list(df_numeric.columns)
        if not cols:
            fig = plt.figure(figsize=(6, 3))
            plt.text(0.5, 0.5, "No numeric columns to plot", ha="center", va="center")
            return fig

        ncols = len(cols)
        fig = plt.figure(figsize=(max(6, ncols * 1.2), 6))
        gs = fig.add_gridspec(2, 1, height_ratios=[1, 1.2], hspace=0.35)

        # Top: boxplot
        ax_box = fig.add_subplot(gs[0, 0])
        df_numeric.boxplot(column=cols, ax=ax_box, showfliers=False)
        ax_box.set_title("Box plot (detected outliers overlaid)")
        ax_box.set_xlabel("")
        ax_box.set_ylabel("Value")

        xs = np.arange(1, len(cols) + 1)
        for i, col in enumerate(cols):
            mask = outlier_masks.get(col)
            if mask is None:
                continue
            out_vals = df_numeric.loc[mask, col]
            if out_vals.empty:
                continue
            # slight horizontal jitter for readability
            jitter = np.random.normal(scale=0.05, size=len(out_vals))
            ax_box.scatter(np.full(len(out_vals), xs[i]) + jitter, out_vals.values,
                           marker='x', s=50, linewidths=1.0, zorder=6)

        # Bottom: scatter plot (index vs value) per column
        ax_scatter = fig.add_subplot(gs[1, 0])
        # Plot each column as its own series using the DataFrame index as x
        for i, col in enumerate(cols):
            series = df_numeric[col]
            mask = outlier_masks.get(col, pd.Series(False, index=series.index))
            # Small x-offset per column to avoid overlap when multiple columns share indices
            x_offset = (i - (ncols - 1) / 2) * 0.08
            xs_plot = series.index.values.astype(float) + x_offset
            ax_scatter.scatter(xs_plot, series.values, alpha=0.6, label=col, s=20)
            # highlight outliers in red with larger marker
            if mask.any():
                ax_scatter.scatter(series.index.values.astype(float)[mask], series[mask].values,
                                   color='red', edgecolors='k', s=50, label=f"{col} outlier", zorder=7)

        ax_scatter.set_title("Scatter plot (index vs value) — outliers highlighted")
        ax_scatter.set_xlabel("Row index")
        ax_scatter.set_ylabel("Value")
        # avoid duplicate legend entries
        handles, labels = ax_scatter.get_legend_handles_labels()
        by_label = dict(zip(labels, handles))
        ax_scatter.legend(by_label.values(), by_label.keys(), fontsize='small', loc='best', ncol=2)

        return fig

    # -------------------- metadata / DB helpers (best-effort) --------------------
    def _get_mariadb_client(self, kernel):
        """Return mariadb_client if present on kernel, else None"""
        return getattr(kernel, "mariadb_client", None)

    def _get_logger(self, kernel):
        """Return a logger on kernel if present, else create a temporary logger"""
        return getattr(kernel, "log", logging.getLogger(__name__))

    def _sql_escape(self, val):
        """Escape a value for SQL single-quoted literal insert. None -> NULL"""
        if val is None:
            return "NULL"
        if not isinstance(val, str):
            val = str(val)
        return "'" + val.replace("'", "''") + "'"

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
                df_list = pd.read_html(result)
                if df_list and isinstance(df_list, list) and len(df_list) > 0:
                    val = df_list[0].iloc[0, 0]
                    if isinstance(val, float) and pd.isna(val):
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

    def _ensure_metadata_table(self, kernel, db_name):
        """
        Create magic_metadata table if it doesn't exist.
        Columns: id, command_name, arguments, execution_timestamp,
                 affected_columns, operation_status, message, db_name, user_name
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
                log.error("Error creating magic_metadata table.")
        except Exception as e:
            log.error(f"Failed to ensure magic_metadata table: {e}")

    def _insert_metadata(self, kernel, command_name, arguments, affected_columns,
                         operation_status, message, db_name, user_name):
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
        """Execute the outliers magic (non in-place) and log metadata."""
        df = data.get("last_select")
        # Prepare metadata context early so we can log failures
        db_name = self._get_db_name(kernel)
        user_name = self._get_user_name(kernel)
        try:
            self._ensure_metadata_table(kernel, db_name)
        except Exception:
            try:
                kernel._send_message("stdout", "Warning: failed to ensure metadata table (continuing).")
            except Exception:
                pass

        if df is None:
            msg = "No last_select found in kernel data."
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

        if hasattr(df, "empty") and df.empty:
            msg = "There is no data to process (empty DataFrame)."
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

        # parse columns argument
        columns_arg = args.get("columns", None)
        if isinstance(columns_arg, str):
            columns = [c.strip() for c in columns_arg.split(",") if c.strip()]
        elif isinstance(columns_arg, (list, tuple)):
            columns = list(columns_arg)
        else:
            columns = None

        method = str(args.get("method", "iqr")).lower()
        if method not in {"iqr", "zscore"}:
            msg = f"Unknown method '{method}'. Allowed: iqr, zscore."
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
            k = float(args.get("k", 1.5))
        except Exception:
            k = 1.5

        try:
            z_thresh = float(args.get("z_thresh", 3.0))
        except Exception:
            z_thresh = 3.0

        plot = bool(args.get("plot", False))

        # Determine target numeric columns
        if columns is None:
            target_columns = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        else:
            missing_cols = [c for c in columns if c not in df.columns]
            if missing_cols:
                msg = f"Column(s) not found: {', '.join(missing_cols)}"
                kernel._send_message("stderr", msg)
                try:
                    self._insert_metadata(
                        kernel=kernel,
                        command_name=self.name(),
                        arguments=self.args if isinstance(self.args, str) else str(self.args),
                        affected_columns="\n".join(columns) if columns else "",
                        operation_status="error",
                        message=msg,
                        db_name=db_name,
                        user_name=user_name
                    )
                except Exception:
                    pass
                return
            # keep only numeric columns (skip non-numeric)
            target_columns = [c for c in columns if pd.api.types.is_numeric_dtype(df[c])]
            non_numeric = [c for c in columns if c not in target_columns]
            if non_numeric:
                kernel._send_message("stdout", f"Warning: non-numeric columns skipped: {', '.join(non_numeric)}")

        if not target_columns:
            msg = "No numeric target columns found to detect outliers."
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

        # Work on a copy (non in-place)
        result_df = df.copy(deep=True)

        # Detect outliers per column and store masks
        outlier_masks = {}
        messages = []
        operation_status = "success"
        try:
            for col in target_columns:
                try:
                    mask = self._detect_outliers_series(result_df[col], method, k=k, z_thresh=z_thresh)
                    outlier_masks[col] = mask
                    n_out = int(mask.sum())
                    messages.append(f"Column '{col}': detected {n_out} outlier(s) using {method}.")
                    # add boolean indicator column to the copy (non in-place on original)
                    result_df[f"{col}_is_outlier"] = mask.astype(bool)
                except Exception as e:
                    messages.append(f"Column '{col}': error detecting outliers: {e}")
        except Exception as e:
            operation_status = "error"
            messages.append(f"Fatal error while detecting outliers: {e}")

        # Store result in a separate key so original remains unchanged
        data["last_select_outliers"] = result_df

        # Send summary message
        kernel._send_message("stdout", "Outlier detection completed (non in-place). Summary:\n" + "\n".join(messages))
        kernel._send_message("stdout", "Results stored in data['last_select_outliers'] (original data['last_select'] unchanged).")

        # Plot if requested
        plot_error = None
        if plot:
            try:
                df_numeric = result_df[target_columns]
                fig = self._build_plots(df_numeric, outlier_masks)
                self._send_image(kernel, fig)
            except Exception as e:
                plot_error = f"Error while plotting: {e}"
                kernel._send_message("stderr", plot_error)
                messages.append(plot_error)
                operation_status = "error"

        # Finally show the result DataFrame (the copy with indicator columns)
        try:
            self._send_html(kernel, data["last_select_outliers"])
        except Exception:
            pass

        # Insert metadata (best-effort)
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
