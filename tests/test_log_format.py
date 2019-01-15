# test_log_format.py
"""Unit tests for lta/log_format.py."""

from lta.log_format import StructuredFormatter


class ObjectLiteral:
    """
    ObjectLiteral transforms named arguments into object attributes.

    This is useful for creating object literals to be used as return
    values from mocked API calls.

    Source: https://stackoverflow.com/a/3335732
    """

    def __init__(self, **kwds):
        """Add attributes to ourself with the provided named arguments."""
        self.__dict__.update(kwds)


class LiteralRecord(ObjectLiteral):
    """
    LiteralRecord is a literal LogRecord.

    This class creates an ObjectLiteral that also implements the (getMessage)
    method which is often called on LogRecord objects.

    This is useful for creating LogRecord literals to be used as return
    values from mocked API calls.
    """

    def getMessage(self):
        """Format the log message"""
        return self.msg % self.args


def test_constructor_default() -> None:
    """Test that StructuredFormatter can be created without any parameters."""
    sf = StructuredFormatter()
    assert sf.component_type is None
    assert sf.component_name is None
    assert sf.indent is None
    assert sf.separators == (',', ':')


def test_constructor_supplied() -> None:
    """Test that StructuredFormatter can be created with parameters."""
    sf = StructuredFormatter(component_type="Picker", component_name="test-picker", ndjson=False)
    assert sf.component_type == "Picker"
    assert sf.component_name == "test-picker"
    assert sf.indent == 4
    assert sf.separators == (', ', ': ')


def test_format_default() -> None:
    """Test that StructuredFormatter (no params) provides proper output."""
    sf = StructuredFormatter()
    log_record = LiteralRecord(
        name="lta.picker",
        msg="ConnectionError trying to PATCH /status/picker with heartbeat",
        args=[],
        levelname="ERROR",
        levelno=40,
        pathname="/home/pmeade/github/lta/lta/picker.py",
        filename="picker.py",
        module="picker",
        exc_info=None,
        exc_text=None,
        stack_info=None,
        lineno=102,
        funcName="patch_status_heartbeat",
        created=1547003161.046467,
        msecs=46.46706581115723,
        relativeCreated=93.13035011291504,
        thread=140013641434880,
        threadName="MainThread",
        processName="MainProcess",
        process=8147
    )
    json_text = sf.format(log_record)
    assert json_text.startswith("{")
    assert json_text.endswith("}")
    assert json_text.find("\n") == -1
    assert json_text.find("component_type") == -1
    assert json_text.find("component_name") == -1
    assert json_text.find("timestamp") != -1


def test_format_supplied() -> None:
    """Test that StructuredFormatter (with params) provides proper output."""
    sf = StructuredFormatter(component_type="Picker", component_name="test-picker", ndjson=False)
    log_record = LiteralRecord(
        name="lta.picker",
        msg="ConnectionError trying to PATCH /status/picker with heartbeat",
        args=[],
        levelname="ERROR",
        levelno=40,
        pathname="/home/pmeade/github/lta/lta/picker.py",
        filename="picker.py",
        module="picker",
        exc_info=None,
        exc_text=None,
        stack_info=None,
        lineno=102,
        funcName="patch_status_heartbeat",
        created=1547003161.046467,
        msecs=46.46706581115723,
        relativeCreated=93.13035011291504,
        thread=140013641434880,
        threadName="MainThread",
        processName="MainProcess",
        process=8147
    )
    json_text = sf.format(log_record)
    assert json_text.startswith("{")
    assert json_text.endswith("}")
    assert json_text.find("\n") != -1
    assert json_text.find("component_type") != -1
    assert json_text.find("component_name") != -1
    assert json_text.find("timestamp") != -1
