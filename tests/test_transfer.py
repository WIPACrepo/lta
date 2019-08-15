# test_transfer.py
"""Module to test TransferService implementations."""

import pytest  # type: ignore

from .test_util import ObjectLiteral
from lta.transfer.service import instantiate, TransferService


def test_instantiate_empty():
    """Fail with a KeyError if the configuration object is an empty dict."""
    with pytest.raises(KeyError):
        instantiate({})

def test_instantiate_module(mocker):
    """Test that instantiate loads the correct module."""
    TRANSFER_CONFIG = {
        "name": "lta.transfer.cp.CopyTransferService",
    }
    mock_import_module = mocker.patch("importlib.import_module")
    mock_import_module.return_value = ObjectLiteral(
        CopyTransferService=TransferService
    )
    xfer_service = instantiate(TRANSFER_CONFIG)
    mock_import_module.assert_called_with("lta.transfer.cp")
    assert isinstance(xfer_service, TransferService)
    assert xfer_service.config == TRANSFER_CONFIG
