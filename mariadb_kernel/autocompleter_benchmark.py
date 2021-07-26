from time import sleep
from typing import List, Type

from prompt_toolkit.completion.base import Completion
from mariadb_kernel.mariadb_client import MariaDBClient
from mariadb_kernel.mariadb_server import MariaDBServer

from mariadb_kernel.mariadb_server import MariaDBServer
from mariadb_kernel.client_config import ClientConfig

from unittest.mock import Mock
from mariadb_kernel.autocompleter import Autocompleter

def get_text_list(completions: List[Completion]):
    return [completion.text for completion in completions]


mocklog = Mock()
cfg = ClientConfig(mocklog)  # default config

mariadb_server = MariaDBServer(mocklog, cfg)
mariadb_server.start()
try:
    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("use test;")
    autocompleter = Autocompleter(client, mocklog)

    from time import time
    start = time()
    for i in range(10):
        autocompleter.refresh(sync=False)
        get_text_list(autocompleter.get_suggestions("sel", 3))
    end = time()
    print(f"time : {end - start} s")
    sleep(1)
    autocompleter.refresh(sync=False)
    get_text_list(autocompleter.get_suggestions("sel", 3))
    while True:
        sleep(0.5)
    
except Exception as e:
    mariadb_server.stop()
    raise e
