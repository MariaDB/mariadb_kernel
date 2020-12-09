from unittest.mock import Mock, ANY
from pandas import DataFrame

from ..maria_magics.line_magic import LineMagic

def test_line_magic_generate_plot_detects_empty_dataframe():
    mockkernel = Mock()
    lm = LineMagic()
    assert lm.type() == "Line"

    data = {'last_select': DataFrame()}
    lm.generate_plot(mockkernel, data, 'testplot')

    mockkernel._send_message.assert_called_once_with('stderr', ANY)


def test_line_magic_generate_plot_detects_ill_formatted_args():
    mockkernel = Mock()
    lm = LineMagic()
    # invalid args
    lm.args = "someplottype"

    data = {'last_select': DataFrame([1,1])}
    lm.generate_plot(mockkernel, data, 'testplot')

    mockkernel._send_message.assert_called_once_with('stderr',
            "There was an error while parsing the arguments. "
            "Please check %lsmagic on how to use the magic command"
    )

def test_line_magic_generate_plot_sends_error_when_plot_throws():
    mockkernel = Mock()
    lm = LineMagic()
    # valid args
    lm.args = "input=1"

    data = {'last_select': DataFrame([1,1])}
    lm.generate_plot(mockkernel, data, 'testplot')

    # The type of plot is invalid
    mockkernel._send_message.assert_called_once_with('stderr', ANY)

def test_line_magic_generate_plot_sends_error_when_index_invalid():
    mockkernel = Mock()
    lm = LineMagic()
    lm.args = 'index="col"'

    data = {'last_select': DataFrame([1,1])}
    lm.generate_plot(mockkernel, data, 'pie')

    mockkernel._send_message.assert_called_once_with('stderr',
        'Index does not exist'
    )

def test_line_magic_generate_plot_sends_display_data():
    mockkernel = Mock()
    lm = LineMagic()
    lm.args = ''

    data = {'last_select': DataFrame([1,1])}
    lm.generate_plot(mockkernel, data, 'line')

    mockkernel.send_response.assert_called_once_with(ANY, 'display_data', ANY)

