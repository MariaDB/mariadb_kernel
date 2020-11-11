import os
import pytest
from unittest.mock import patch, Mock

from ..client_config import ClientConfig


@patch.object(ClientConfig, "_config_path", return_value="./mariadb_config.json")
def test_client_config_file_doesnt_exist(mock_config_path):
    mocklog = Mock()
    cfg = ClientConfig(mocklog)

    mock_config_path.assert_called()

    # No config file found, we didn't create any
    mocklog.info.assert_any_call(
        "Config file mariadb_config.json at ./mariadb_config.json does not exist"
    )
    mocklog.info.assert_any_call(f"Using default config: {cfg.default_config}")


@patch.object(ClientConfig, "_config_path", return_value="./mariadb_config.json")
def test_client_config_invalid_json(mock_config_path):
    invalid_json = "{"

    # Create a config file containing invalid JSON
    with open("mariadb_config.json", "w") as json_file:
        json_file.write(invalid_json)

    mocklog = Mock()
    cfg = ClientConfig(mocklog)

    # Cleanup
    os.remove("mariadb_config.json")

    mocklog.info.assert_any_call(
        "Config file mariadb_config.json at ./mariadb_config.json is not valid JSON: "
        "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)"
    )
    mocklog.info.assert_any_call(f"Using default config: {cfg.default_config}")


@patch.object(ClientConfig, "_config_path", return_value="./mariadb_config.json")
def test_client_config_unsupported_options(mock_config_path):
    # test_config="""
    # {"user": "testuser",
    # "host": "testhost",
    # "start_server": "False",
    # "password": "secret_password",
    # "client_bin": "/test/path/mariadb",
    # "server_bin": "/test/path/mariadbd"}
    # """
    json = '{"unsupported_option": "unsupported"}'

    # Create a config file containing some unsupported options
    with open("mariadb_config.json", "w") as json_file:
        json_file.write(json)

    mocklog = Mock()
    cfg = ClientConfig(mocklog)

    # Cleanup
    os.remove("mariadb_config.json")

    mocklog.info.assert_any_call(
        "Config file mariadb_config.json at ./mariadb_config.json "
        "contains unsupported options: {'unsupported_option'}"
    )
    mocklog.info.assert_any_call(f"Using default config: {cfg.default_config}")


@patch.object(ClientConfig, "_config_path", return_value="./mariadb_config.json")
def test_client_config_loads_correct_values(mock_config_path):
    json = """
    {"user": "testuser",
     "host": "testhost",
     "port": "00",
     "start_server": "False",
     "password": "secret_password",
     "client_bin": "/test/path/mariadb",
     "server_bin": "/test/path/mariadbd"}
     """

    # Create a proper config file
    with open("mariadb_config.json", "w") as json_file:
        json_file.write(json)

    mocklog = Mock()
    cfg = ClientConfig(mocklog)

    # Cleanup
    os.remove("mariadb_config.json")

    mocklog.info.assert_called_once_with(
        f"Loading config file at ./mariadb_config.json..."
    )

    assert (
        cfg.get_args()
        == "--user=testuser --host=testhost --port=00 --password=secret_password "
    )
    assert cfg.start_server() == False
    assert cfg.client_bin() == "/test/path/mariadb"
    assert cfg.server_bin() == "/test/path/mariadbd"


def test_client_config_path():
    cfg = ClientConfig(Mock())

    # ClientConfig should always look by default for the config file in ~/.jupyter
    with patch.dict("os.environ", {"JUPYTER_CONFIG_DIR": ""}):
        assert cfg._config_path() == os.path.join(
            os.path.expanduser("~"), ".jupyter/mariadb_config.json"
        )

    with patch.dict("os.environ", {"JUPYTER_CONFIG_DIR": "/test/"}):
        assert cfg._config_path() == "/test/mariadb_config.json"
