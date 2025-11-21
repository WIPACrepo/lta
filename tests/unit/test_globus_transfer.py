"""Tests for lta.transfer.globus.GlobusTransfer"""

import dataclasses
import datetime
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from globus_sdk import GlobusHTTPResponse
from requests import Response

from lta.transfer.globus import (
    GlobusTransfer,
    GlobusTransferEnv,
    GlobusTransferFailedException,
)


class _FakeResponse:
    """This mimics the requests.Response class for our purposes."""

    def __init__(self, data: dict):
        self._data = data
        self.status_code = 200
        self.headers = {"Content-Type": "application/json"}
        self.text = (
            "THIS IS A FAKE RESPONSE -- IF REAL TEXT IS NEEDED, UPDATE THIS CLASS"
        )

    def json(self) -> dict:
        return self._data


# ---------------------------------------------------------------------------
# GlobusTransferEnv
# ---------------------------------------------------------------------------


def test_000_globus_transfer_env_defaults() -> None:
    """GlobusTransferEnv applies default values and is frozen."""
    # arrange: inputs
    env = GlobusTransferEnv(
        GLOBUS_CLIENT_ID="cid",
        GLOBUS_CLIENT_SECRET="secret",
        GLOBUS_SOURCE_COLLECTION_ID="src-id",
        GLOBUS_DEST_COLLECTION_ID="dst-id",
    )

    # assert: defaults
    assert (
        env.GLOBUS_TRANSFER_SCOPE == "urn:globus:auth:scope:transfer.api.globus.org:all"
    )
    assert env.GLOBUS_POLL_INTERVAL_SECONDS == 60
    assert env.GLOBUS_HARD_DEADLINE_SECONDS is None

    # act + assert: immutability
    with pytest.raises(dataclasses.FrozenInstanceError):
        env.GLOBUS_CLIENT_ID = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# __init__ / _create_client
# ---------------------------------------------------------------------------


@patch("lta.transfer.globus.globus_sdk.AccessTokenAuthorizer")
@patch("lta.transfer.globus.globus_sdk.TransferClient")
@patch("lta.transfer.globus.globus_sdk.ConfidentialAppAuthClient")
@patch("lta.transfer.globus.from_environment_as_dataclass")
def test_100_globus_transfer_init_wires_sdk_correctly(
    mock_from_env,
    mock_confidential,
    mock_transfer_client,
    mock_authorizer,
) -> None:
    """__init__ loads env and builds a TransferClient with an access token."""
    # arrange: environment + SDK patches
    mock_from_env.return_value = GlobusTransferEnv(
        GLOBUS_CLIENT_ID="cid",
        GLOBUS_CLIENT_SECRET="secret",
        GLOBUS_SOURCE_COLLECTION_ID="src-id",
        GLOBUS_DEST_COLLECTION_ID="dst-id",
        GLOBUS_TRANSFER_SCOPE="scope:transfer",
        GLOBUS_POLL_INTERVAL_SECONDS=5,
    )

    token_resp = MagicMock()
    token_resp.by_resource_server = {
        "transfer.api.globus.org": {"access_token": "ACCESS-TOKEN"},
    }
    mock_confidential.return_value.oauth2_client_credentials_tokens.return_value = (
        token_resp
    )

    # act
    gt = GlobusTransfer()

    # assert: env + auth wiring
    mock_from_env.assert_called_once_with(GlobusTransferEnv)
    mock_confidential.assert_called_once_with("cid", "secret")
    mock_confidential.return_value.oauth2_client_credentials_tokens.assert_called_once_with(
        requested_scopes="scope:transfer",
    )

    # assert: authorizer + client creation
    mock_authorizer.assert_called_once_with("ACCESS-TOKEN")
    mock_transfer_client.assert_called_once_with(
        authorizer=mock_authorizer.return_value,
    )
    assert gt._transfer_client is mock_transfer_client.return_value


# ---------------------------------------------------------------------------
# make_transfer_document
# ---------------------------------------------------------------------------


@patch("lta.transfer.globus.globus_sdk.AccessTokenAuthorizer")
@patch("lta.transfer.globus.globus_sdk.TransferClient")
@patch("lta.transfer.globus.globus_sdk.ConfidentialAppAuthClient")
@patch("lta.transfer.globus.from_environment_as_dataclass")
def test_200_make_transfer_document_builds_expected_transferdata(
    mock_from_env,
    mock_confidential,
    mock_transfer_client,
    mock_authorizer,
) -> None:
    """make_transfer_document builds TransferData with correct label and deadline."""
    # arrange: environment + SDK patches
    deadline = 60
    mock_from_env.return_value = GlobusTransferEnv(
        GLOBUS_CLIENT_ID="cid",
        GLOBUS_CLIENT_SECRET="secret",
        GLOBUS_SOURCE_COLLECTION_ID="src-id",
        GLOBUS_DEST_COLLECTION_ID="dst-id",
        GLOBUS_HARD_DEADLINE_SECONDS=deadline,
        GLOBUS_POLL_INTERVAL_SECONDS=5,
    )

    token_resp = MagicMock()
    token_resp.by_resource_server = {
        "transfer.api.globus.org": {"access_token": "ACCESS-TOKEN"},
    }
    mock_confidential.return_value.oauth2_client_credentials_tokens.return_value = (
        token_resp
    )

    # arrange: inputs
    source = Path("/absolute/source.dat")
    dest = Path("/dest/collection/path.dat")

    # act
    gt = GlobusTransfer()
    before = datetime.datetime.now(datetime.timezone.utc)
    tdata = gt.make_transfer_document(source, dest)
    after = datetime.datetime.now(datetime.timezone.utc)

    # assert: basic transfer metadata
    assert tdata["source_endpoint"] == "src-id"
    assert tdata["destination_endpoint"] == "dst-id"
    assert tdata["fail_on_quota_errors"] is True
    # Globus maps sync level "mtime" → numeric code 2 internally.
    assert tdata["sync_level"] == 2

    # assert: label content
    assert tdata["label"] == f"LTA bundle: {source.name}"

    # assert: deadline window (account for seconds truncation)
    base_time = datetime.datetime.fromisoformat(tdata["deadline"]) - datetime.timedelta(
        seconds=deadline
    )
    fuzz = datetime.timedelta(seconds=1)
    assert (before - fuzz) <= base_time <= after

    # assert: transfer items
    items = tdata["DATA"]
    assert len(items) == 1
    item = items[0]
    assert item["source_path"] == str(source)
    assert item["destination_path"] == str(dest)


# ---------------------------------------------------------------------------
# _submit_transfer
# ---------------------------------------------------------------------------


@patch("lta.transfer.globus.asyncio.sleep", new_callable=AsyncMock)
@patch("lta.transfer.globus.globus_sdk.AccessTokenAuthorizer")
@patch("lta.transfer.globus.globus_sdk.TransferClient")
@patch("lta.transfer.globus.globus_sdk.ConfidentialAppAuthClient")
@patch("lta.transfer.globus.from_environment_as_dataclass")
@pytest.mark.asyncio
async def test_300_submit_transfer_uses_client_and_returns_task_id(
    mock_from_env,
    mock_confidential,
    mock_transfer_client,
    mock_authorizer,
    mock_sleep,
) -> None:
    """_submit_transfer calls submit_transfer and returns the task_id."""
    # arrange: environment + SDK patches
    mock_from_env.return_value = GlobusTransferEnv(
        GLOBUS_CLIENT_ID="cid",
        GLOBUS_CLIENT_SECRET="secret",
        GLOBUS_SOURCE_COLLECTION_ID="src-id",
        GLOBUS_DEST_COLLECTION_ID="dst-id",
    )

    token_resp = MagicMock()
    token_resp.by_resource_server = {
        "transfer.api.globus.org": {"access_token": "ACCESS-TOKEN"},
    }
    mock_confidential.return_value.oauth2_client_credentials_tokens.return_value = (
        token_resp
    )

    client = MagicMock()
    client.submit_transfer.return_value = GlobusHTTPResponse(
        cast(Response, _FakeResponse({"task_id": "TASK-123"})),
        client=MagicMock(),
    )
    mock_transfer_client.return_value = client

    # arrange: inputs
    tdata = MagicMock()

    # act
    gt = GlobusTransfer()
    tid = await gt._submit_transfer(tdata)

    # assert: client call + cooperative yield
    client.submit_transfer.assert_called_once_with(tdata)
    mock_sleep.assert_awaited_once_with(0)
    assert tid == "TASK-123"


# ---------------------------------------------------------------------------
# transfer_file – public API
# ---------------------------------------------------------------------------


@patch("lta.transfer.globus.globus_sdk.AccessTokenAuthorizer")
@patch("lta.transfer.globus.globus_sdk.TransferClient")
@patch("lta.transfer.globus.globus_sdk.ConfidentialAppAuthClient")
@patch("lta.transfer.globus.from_environment_as_dataclass")
@pytest.mark.asyncio
async def test_400_transfer_file_rejects_relative_source_path(
    mock_from_env,
    mock_confidential,
    mock_transfer_client,
    mock_authorizer,
) -> None:
    """transfer_file enforces absolute source_path."""
    # arrange: environment + SDK patches
    mock_from_env.return_value = GlobusTransferEnv(
        GLOBUS_CLIENT_ID="cid",
        GLOBUS_CLIENT_SECRET="secret",
        GLOBUS_SOURCE_COLLECTION_ID="src-id",
        GLOBUS_DEST_COLLECTION_ID="dst-id",
    )

    token_resp = MagicMock()
    token_resp.by_resource_server = {
        "transfer.api.globus.org": {"access_token": "ACCESS-TOKEN"},
    }
    mock_confidential.return_value.oauth2_client_credentials_tokens.return_value = (
        token_resp
    )
    mock_transfer_client.return_value = MagicMock()

    # act + assert: relative path rejected
    with pytest.raises(ValueError) as excinfo:
        await GlobusTransfer().transfer_file(
            source_path=Path("relative/path.dat"),
            dest_path=Path("/dest/path.dat"),
        )
    assert "must be absolute" in str(excinfo.value)


@patch("lta.transfer.globus.globus_sdk.AccessTokenAuthorizer")
@patch("lta.transfer.globus.globus_sdk.TransferClient")
@patch("lta.transfer.globus.globus_sdk.ConfidentialAppAuthClient")
@patch("lta.transfer.globus.from_environment_as_dataclass")
@pytest.mark.asyncio
async def test_410_transfer_file_success_on_first_poll(
    mock_from_env,
    mock_confidential,
    mock_transfer_client,
    mock_authorizer,
) -> None:
    """If first status is SUCCEEDED, transfer_file returns immediately."""
    # arrange: environment + SDK patches
    mock_from_env.return_value = GlobusTransferEnv(
        GLOBUS_CLIENT_ID="cid",
        GLOBUS_CLIENT_SECRET="secret",
        GLOBUS_SOURCE_COLLECTION_ID="src-id",
        GLOBUS_DEST_COLLECTION_ID="dst-id",
        GLOBUS_POLL_INTERVAL_SECONDS=1,
    )

    token_resp = MagicMock()
    token_resp.by_resource_server = {
        "transfer.api.globus.org": {"access_token": "ACCESS-TOKEN"},
    }
    mock_confidential.return_value.oauth2_client_credentials_tokens.return_value = (
        token_resp
    )

    client = MagicMock()
    client.submit_transfer.return_value = GlobusHTTPResponse(
        cast(Response, _FakeResponse({"task_id": "TASK-123"})), client=MagicMock()
    )
    client.get_task.return_value = GlobusHTTPResponse(
        cast(Response, _FakeResponse({"status": "SUCCEEDED"})), client=MagicMock()
    )
    mock_transfer_client.return_value = client

    # act
    gt = GlobusTransfer()
    task_id = await gt.transfer_file(
        source_path=Path("/abs/path.dat"),
        dest_path=Path("/dest/path.dat"),
    )
    await gt.wait_for_transfer_to_finish(task_id)

    # assert: submit + single poll
    client.submit_transfer.assert_called_once()
    client.get_task.assert_called_once_with("TASK-123")
    assert task_id == "TASK-123"


@patch("lta.transfer.globus.asyncio.sleep", new_callable=AsyncMock)
@patch("lta.transfer.globus.globus_sdk.AccessTokenAuthorizer")
@patch("lta.transfer.globus.globus_sdk.TransferClient")
@patch("lta.transfer.globus.globus_sdk.ConfidentialAppAuthClient")
@patch("lta.transfer.globus.from_environment_as_dataclass")
@pytest.mark.asyncio
async def test_420_transfer_file_active_then_succeeds(
    mock_from_env,
    mock_confidential,
    mock_transfer_client,
    mock_authorizer,
    mock_sleep,
) -> None:
    """ACTIVE status causes a poll + sleep, then SUCCEEDED returns task_id."""
    # arrange: environment + SDK patches
    poll_interval = 2
    mock_from_env.return_value = GlobusTransferEnv(
        GLOBUS_CLIENT_ID="cid",
        GLOBUS_CLIENT_SECRET="secret",
        GLOBUS_SOURCE_COLLECTION_ID="src-id",
        GLOBUS_DEST_COLLECTION_ID="dst-id",
        GLOBUS_POLL_INTERVAL_SECONDS=poll_interval,
    )

    token_resp = MagicMock()
    token_resp.by_resource_server = {
        "transfer.api.globus.org": {"access_token": "ACCESS-TOKEN"},
    }
    mock_confidential.return_value.oauth2_client_credentials_tokens.return_value = (
        token_resp
    )

    client = MagicMock()
    client.submit_transfer.return_value = GlobusHTTPResponse(
        cast(Response, _FakeResponse({"task_id": "TASK-123"})), client=MagicMock()
    )
    client.get_task.side_effect = [
        GlobusHTTPResponse(
            cast(Response, _FakeResponse({"status": "ACTIVE"})), client=MagicMock()
        ),
        GlobusHTTPResponse(
            cast(Response, _FakeResponse({"status": "SUCCEEDED"})), client=MagicMock()
        ),
    ]
    mock_transfer_client.return_value = client

    # act
    gt = GlobusTransfer()
    task_id = await gt.transfer_file(
        source_path=Path("/abs/file.dat"),
        dest_path=Path("/dest/path.dat"),
    )
    await gt.wait_for_transfer_to_finish(task_id)

    # assert: two polls + expected sleeps (0 from _submit_transfer, then poll_interval)
    assert client.get_task.call_count == 2
    assert mock_sleep.await_count == 2
    first_call_args = mock_sleep.await_args_list[0].args
    second_call_args = mock_sleep.await_args_list[1].args
    assert first_call_args == (0,)
    assert second_call_args == (poll_interval,)
    assert task_id == "TASK-123"


@patch("lta.transfer.globus.globus_sdk.AccessTokenAuthorizer")
@patch("lta.transfer.globus.globus_sdk.TransferClient")
@patch("lta.transfer.globus.globus_sdk.ConfidentialAppAuthClient")
@patch("lta.transfer.globus.from_environment_as_dataclass")
@pytest.mark.asyncio
async def test_440_transfer_file_failed_raises(
    mock_from_env,
    mock_confidential,
    mock_transfer_client,
    mock_authorizer,
) -> None:
    """FAILED status raises GlobusTransferFailedException."""
    # arrange: environment + SDK patches
    mock_from_env.return_value = GlobusTransferEnv(
        GLOBUS_CLIENT_ID="cid",
        GLOBUS_CLIENT_SECRET="secret",
        GLOBUS_SOURCE_COLLECTION_ID="src-id",
        GLOBUS_DEST_COLLECTION_ID="dst-id",
    )

    token_resp = MagicMock()
    token_resp.by_resource_server = {
        "transfer.api.globus.org": {"access_token": "ACCESS-TOKEN"},
    }
    mock_confidential.return_value.oauth2_client_credentials_tokens.return_value = (
        token_resp
    )

    client = MagicMock()
    client.submit_transfer.return_value = GlobusHTTPResponse(
        cast(Response, _FakeResponse({"task_id": "TASK-123"})), client=MagicMock()
    )
    client.get_task.return_value = GlobusHTTPResponse(
        cast(Response, _FakeResponse({"status": "FAILED"})), client=MagicMock()
    )
    mock_transfer_client.return_value = client

    # act
    gt = GlobusTransfer()
    task_id = await gt.transfer_file(
        source_path=Path("/abs/file.dat"),
        dest_path=Path("/dest/path.dat"),
    )
    with pytest.raises(GlobusTransferFailedException) as excinfo:
        await gt.wait_for_transfer_to_finish(task_id)

    # assert: failure surface
    text = str(excinfo.value)
    assert "FAILED" in text
    assert "TASK-123" in text
    client.submit_transfer.assert_called_once()
    client.get_task.assert_called_once_with("TASK-123")


@patch("lta.transfer.globus.globus_sdk.AccessTokenAuthorizer")
@patch("lta.transfer.globus.globus_sdk.TransferClient")
@patch("lta.transfer.globus.globus_sdk.ConfidentialAppAuthClient")
@patch("lta.transfer.globus.from_environment_as_dataclass")
@pytest.mark.asyncio
async def test_450_transfer_file_inactive_raises(
    mock_from_env,
    mock_confidential,
    mock_transfer_client,
    mock_authorizer,
) -> None:
    """INACTIVE status raises GlobusTransferFailedException."""
    # arrange: environment + SDK patches
    mock_from_env.return_value = GlobusTransferEnv(
        GLOBUS_CLIENT_ID="cid",
        GLOBUS_CLIENT_SECRET="secret",
        GLOBUS_SOURCE_COLLECTION_ID="src-id",
        GLOBUS_DEST_COLLECTION_ID="dst-id",
    )

    token_resp = MagicMock()
    token_resp.by_resource_server = {
        "transfer.api.globus.org": {"access_token": "ACCESS-TOKEN"},
    }
    mock_confidential.return_value.oauth2_client_credentials_tokens.return_value = (
        token_resp
    )

    client = MagicMock()
    client.submit_transfer.return_value = GlobusHTTPResponse(
        cast(Response, _FakeResponse({"task_id": "TASK-123"})), client=MagicMock()
    )
    client.get_task.return_value = GlobusHTTPResponse(
        cast(Response, _FakeResponse({"status": "INACTIVE"})), client=MagicMock()
    )
    mock_transfer_client.return_value = client

    # act
    gt = GlobusTransfer()
    task_id = await gt.transfer_file(
        source_path=Path("/abs/file.dat"),
        dest_path=Path("/dest/path.dat"),
    )
    with pytest.raises(GlobusTransferFailedException) as excinfo:
        await gt.wait_for_transfer_to_finish(task_id)

    # assert: failure surface
    text = str(excinfo.value)
    assert "INACTIVE" in text
    assert "TASK-123" in text
    client.submit_transfer.assert_called_once()
    client.get_task.assert_called_once_with("TASK-123")


@patch("lta.transfer.globus.asyncio.sleep", new_callable=AsyncMock)
@patch("lta.transfer.globus.globus_sdk.AccessTokenAuthorizer")
@patch("lta.transfer.globus.globus_sdk.TransferClient")
@patch("lta.transfer.globus.globus_sdk.ConfidentialAppAuthClient")
@patch("lta.transfer.globus.from_environment_as_dataclass")
@pytest.mark.asyncio
async def test_460_transfer_file_unknown_status_then_succeeds(
    mock_from_env,
    mock_confidential,
    mock_transfer_client,
    mock_authorizer,
    mock_sleep,
) -> None:
    """Unknown status is ignored and polling continues until success."""
    # arrange: environment + SDK patches
    poll_interval = 1
    mock_from_env.return_value = GlobusTransferEnv(
        GLOBUS_CLIENT_ID="cid",
        GLOBUS_CLIENT_SECRET="secret",
        GLOBUS_SOURCE_COLLECTION_ID="src-id",
        GLOBUS_DEST_COLLECTION_ID="dst-id",
        GLOBUS_POLL_INTERVAL_SECONDS=poll_interval,
    )

    token_resp = MagicMock()
    token_resp.by_resource_server = {
        "transfer.api.globus.org": {"access_token": "ACCESS-TOKEN"},
    }
    mock_confidential.return_value.oauth2_client_credentials_tokens.return_value = (
        token_resp
    )

    client = MagicMock()
    client.submit_transfer.return_value = GlobusHTTPResponse(
        cast(Response, _FakeResponse({"task_id": "TASK-123"})), client=MagicMock()
    )
    client.get_task.side_effect = [
        GlobusHTTPResponse(
            cast(Response, _FakeResponse({"status": "FOO"})), client=MagicMock()
        ),
        GlobusHTTPResponse(
            cast(Response, _FakeResponse({"status": "SUCCEEDED"})), client=MagicMock()
        ),
    ]
    mock_transfer_client.return_value = client

    # act
    gt = GlobusTransfer()
    task_id = await gt.transfer_file(
        source_path=Path("/abs/file.dat"),
        dest_path=Path("/dest/path.dat"),
    )
    await gt.wait_for_transfer_to_finish(task_id)

    # assert: unknown status tolerant
    assert task_id == "TASK-123"
    assert client.get_task.call_count == 2
    # _submit_transfer: sleep(0), then polling sleep(poll_interval)
    assert mock_sleep.await_count == 2
    first_call_args = mock_sleep.await_args_list[0].args
    second_call_args = mock_sleep.await_args_list[1].args
    assert first_call_args == (0,)
    assert second_call_args == (poll_interval,)
