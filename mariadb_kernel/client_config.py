"""ClientConfig deals with the args passed to the MariaDB client"""

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

import os
import json


class ClientConfig:
    def __init__(self, log, name="mariadb_config.json"):
        self.log = log
        self.config_name = name
        self.default_config = {
            "user": "root",
            "host": "localhost",
            "socket": "/tmp/mysqld.sock",
            "port": "3306",
            "password": "",
            "start_server": "True",
            "client_bin": "mysql",
            "server_bin": "mysqld",
            "db_init_bin": "mysql_install_db",
            "extra_server_config": [
                "--pid-file=/tmp/mysqld.pid",
                f"--datadir={os.path.join(os.environ.get('HOME', '/home/jovyan/'), 'work', '.db')}",
                "--skip_log_error",
            ],
            "extra_db_init_config": [
                f"--user={os.environ.get('NB_USER', 'jovyan')}",
                "--auth-root-authentication-method=normal",
                "--skip-test-db",
                "--rpm",
            ],
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
        return rv

    def get_server_args(self):
        rv = []
        rv.extend(self.default_config["extra_server_config"])
        # get_args return a string and we need it as a list
        # we also do not want user as sql user might differ from exec user
        rv.append(f"--socket={self.default_config['socket']}")
        rv.append(f"--port={self.default_config['port']}")
        rv.append(f"--bind-address={self.default_config['host']}")
        return rv

    def get_init_args(self):
        rv = []
        rv.extend(self.get_server_args())
        rv.extend(self.default_config["extra_db_init_config"])
        return rv

    def start_server(self):
        return self.default_config["start_server"] == "True"

    def client_bin(self):
        return self.default_config["client_bin"]

    def server_bin(self):
        return self.default_config["server_bin"]

    def db_init_bin(self):
        return self.default_config["db_init_bin"]
