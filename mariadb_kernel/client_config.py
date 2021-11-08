"""ClientConfig deals with the args passed to the MariaDB client"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

import os
import json


class ClientConfig:
    def __init__(self, log, name="mariadb_config.json"):
        self.log = log
        self.config_name = name

        datadir = "/tmp/mariadb_kernel/datadir"
        pidfile = "/tmp/mariadb_kernel/mysqld.pid"
        socketfile = "/tmp/mariadb_kernel/mysqld.sock"

        if "NB_USER" in os.environ:
            datadir = os.path.join("/home/", os.environ["NB_USER"], "work", ".db")

        self.default_config = {
            "user": "root",
            "host": "localhost",
            "socket": socketfile,
            "port": "3306",
            "password": "",
            "server_datadir": datadir,  # Server specific option
            "server_pid": pidfile,  # Server specific option
            "start_server": "True",
            "client_bin": "mysql",
            "server_bin": "mysqld",
            "db_init_bin": "mysql_install_db",
            "extra_server_config": [
                "--no-defaults",
                "--skip_log_error",
            ],
            "extra_db_init_config": [
                "--auth-root-authentication-method=normal",
            ],
            "debug": "False",
            "code_completion": "True",
        }

        self._load_config()

    def _load_config(self):
        path = self._config_path()
        self.log.info(f"Loading config file at {path}...")
        cfg = {}
        using_default = False

        try:
            f = open(path, "r")
            cfg = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            if isinstance(e, OSError):
                self.log.info(
                    f"Config file {self.config_name} at {path} " "does not exist"
                )
            if isinstance(e, json.JSONDecodeError):
                self.log.info(
                    f"Config file {self.config_name} at {path} "
                    f"is not valid JSON: {e}"
                )
            using_default = True

        # We should abort loading the custom config if the user passes
        # an unsupported option
        customk = cfg.keys()
        defaultk = self.default_config.keys()
        if len(customk - defaultk) > 0:
            self.log.info(
                f"Config file {self.config_name} at {path} "
                f"contains unsupported options: {customk - defaultk}"
            )
            using_default = True

        if using_default:
            self.log.info(
                f"Using default config: {json.dumps(self.default_config, indent=4)}"
            )
            return

        self.default_config.update(cfg)

    def _config_path(self):
        default_dir = os.path.join(os.path.expanduser("~"), ".jupyter")
        custom_dir = os.environ.get("JUPYTER_CONFIG_DIR")
        if custom_dir:
            default_dir = custom_dir
        return os.path.join(default_dir, self.config_name)

    def get_args(self):
        rv = ""
        keys = ["user", "host", "port", "password", "socket"]
        for key in keys:
            value = self.default_config[key]
            rv += f"--{key}={value} "

        # Disable progress reports in statements like LOAD DATA
        rv += "--disable-progress-reports"
        return rv

    def get_server_args(self):
        rv = []
        rv.extend(self.default_config["extra_server_config"])
        # Use same connection config for both server and client
        rv.append(f"--socket={self.default_config['socket']}")
        rv.append(f"--port={self.default_config['port']}")
        rv.append(f"--bind-address={self.default_config['host']}")
        # Server specific config
        rv.append(f"--datadir={self.default_config['server_datadir']}")
        rv.append(f"--pid-file={self.default_config['server_pid']}")
        return rv

    def get_init_args(self):
        rv = []
        rv.extend(self.get_server_args())
        rv.extend(self.default_config["extra_db_init_config"])
        return rv

    def get_server_paths(self):
        return [
            os.path.dirname(self.default_config["socket"]),
            os.path.dirname(self.default_config["server_datadir"]),
            os.path.dirname(self.default_config["server_pid"]),
        ]

    def get_server_pidfile(self):
        return self.default_config["server_pid"]

    def start_server(self):
        return self.default_config["start_server"] == "True"

    def client_bin(self):
        return self.default_config["client_bin"]

    def server_bin(self):
        return self.default_config["server_bin"]

    def db_init_bin(self):
        return self.default_config["db_init_bin"]

    def debug_logging(self):
        return self.default_config["debug"] == "True"

    def autocompletion_enabled(self):
        return self.default_config["code_completion"] == "True"
