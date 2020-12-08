from unittest.mock import Mock

from ..maria_magics import *

def test_magic_factory_returns_the_right_magic(magic_cmd):
    f = magic_factory.MagicFactory(Mock())

    magic = f.create_magic(magic_cmd, '')

    index = supported_magics.get()

    assert type(magic) == index[magic_cmd]

def test_error_magic_sends_error_to_kernel():
    mockkernel = Mock()
    name = 'nonexistent'
    m = magic_factory.ErrorMagic(name)

    m.execute(mockkernel, None)

    mockkernel._send_message.assert_called_once_with('stderr',
            f'The %{name} magic command does not exist')

