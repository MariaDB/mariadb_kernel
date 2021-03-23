import os
import pytest
import shutil
from pathlib import Path
from subprocess import check_output
from ..mariadb_server import MariaDBServer


def cleanup_server_setup():
    cnfpath = Path.home().joinpath(".my.cnf")
    backup = Path.home().joinpath("mycnf_backup")
    if backup.exists():
        backup.replace(cnfpath)
    shutil.rmtree("/tmp/datadir-test")


@pytest.fixture(scope="session", autouse=True)
def server_setup():
    cfg = """
    [client-server]
    socket=/tmp/mysql-dbug.sock
    port=3306

    [mariadb]
    max-connections=20
    pid-file=/tmp/mysqld.pid
    datadir=/tmp/datadir-test
    lc-messages=en_us
    skip_log_error
    """

    cnfpath = Path.home().joinpath(".my.cnf")
    if cnfpath.exists():
        backup = Path.home().joinpath("mycnf_backup")
        cnfpath.rename(backup)

    with cnfpath.open("w") as f:
        f.write(cfg)

    os.mkdir("/tmp/datadir-test")

    try:
        check_output(
            [
                "mysql_install_db",
                "--datadir=/tmp/datadir-test",
                "--auth-root-authentication-method=normal",
            ]
        )
    except:
        cleanup_server_setup()
        raise

    yield

    cleanup_server_setup()


@pytest.fixture
def mariadb_server():
    server = None

    def _server(log, config):
        nonlocal server
        server = MariaDBServer(log, config)
        server.start()
        return server

    yield _server

    server.stop()


@pytest.fixture(params=["line", "bar", "pie", "df", "lsmagic"])
def magic_cmd(request):
    return request.param
