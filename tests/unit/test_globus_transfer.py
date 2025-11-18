"""Tests for lta.transfer.globus.GlobusTransfer"""

import dataclasses
import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from lta.transfer.globus import (
    GlobusTransfer,
    GlobusTransferEnv,
    GlobusTransferFailedException,
)


# ---------------------------------------------------------------------------
# GlobusTransferEnv
# ---------------------------------------------------------------------------


def test_000_globus_transfer_env_defaults() -> None:
    """GlobusTransferEnv applies default values and is frozen."""
    # arrange: inputs
    env = GlobusTransferEnv(
        GLOBUS_CLIENT_ID="cid",
        GLOBUS_CLIENT_SECRET="secret",
        GLOBUS_SOURCE_COLLECTION_ID="src",
        GLOBUS_DEST_COLLECTION_ID="dst",
    )

    # assert: defaults
    assert (
        env.GLOBUS_TRANSFER_SCOPE == "urn:globus:auth:scope:transfer.api.globus.org:all"
    )
    assert env.GLOBUS_POLL_INTERVAL_SECONDS == 10.0

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
    env = GlobusTransferEnv(
        GLOBUS_CLIENT_ID="cid",
        GLOBUS_CLIENT_SECRET="secret",
        GLOBUS_SOURCE_COLLECTION_ID="src-id",
        GLOBUS_DEST_COLLECTION_ID="dst-id",
        GLOBUS_TRANSFER_SCOPE="scope:transfer",
        GLOBUS_POLL_INTERVAL_SECONDS=5.0,
    )
    mock_from_env.return_value = env

    token_resp = MagicMock()
    token_resp.by_resource_server = {
        "transfer.api.globus.org": {"access_token": "ACCESS-TOKEN"}
    }
    mock_confidential.return_value.oauth2_client_credentials_tokens.return_value = (
        token_resp
    )

    # act
    gt = GlobusTransfer()

    # assert
    mock_from_env.assert_called_once_with(GlobusTransferEnv)
    mock_confidential.assert_called_once_with("cid", "secret")
    mock_confidential.return_value.oauth2_client_credentials_tokens.assert_called_once_with(
        requested_scopes="scope:transfer",
    )
    mock_authorizer.assert_called_once_with("ACCESS-TOKEN")
    mock_transfer_client.assert_called_once_with(
        authorizer=mock_authorizer.return_value,
    )

    assert gt._env is env
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
    env = GlobusTransferEnv(
        GLOBUS_CLIENT_ID="cid",
        GLOBUS_CLIENT_SECRET="secret",
        GLOBUS_SOURCE_COLLECTION_ID="src-id",
        GLOBUS_DEST_COLLECTION_ID="dst-id",
        GLOBUS_POLL_INTERVAL_SECONDS=5.0,
    )
    mock_from_env.return_value = env

    token_resp = MagicMock()
    token_resp.by_resource_server = {
        "transfer.api.globus.org": {"access_token": "ACCESS-TOKEN"}
    }
    mock_confidential.return_value.oauth2_client_credentials_tokens.return_value = (
        token_resp
    )

    # arrange: inputs
    source = "/absolute/source.dat"
    dest = "globus://dest-collection/path"
    timeout = 60

    # act
    gt = GlobusTransfer()
    before = datetime.datetime.now(datetime.timezone.utc)
    tdata = gt.make_transfer_document(source, dest, timeout)
    after = datetime.datetime.now(datetime.timezone.utc)

    # assert: basic transfer metadata
    assert tdata["source_endpoint"] == env.GLOBUS_SOURCE_COLLECTION_ID
    assert tdata["destination_endpoint"] == env.GLOBUS_DEST_COLLECTION_ID
    assert tdata["fail_on_quota_errors"] is True
    assert tdata["sync_level"] == "mtime"

    # assert: label content
    label = tdata["label"]
    assert label.startswith("LTA bundle transfer: ")
    assert source in label
    assert dest in label

    # assert: deadline window (with cushion and seconds truncation)
    deadline_str = tdata["deadline"]
    deadline = datetime.datetime.fromisoformat(deadline_str)
    cushion = datetime.timedelta(seconds=env.GLOBUS_POLL_INTERVAL_SECONDS * 5)

    # isoformat(..., timespec="seconds") truncates microseconds, so allow up to
    # ~1 second slack on the lower bound, but still require the cushion.
    min_deadline = (
        before
        + datetime.timedelta(seconds=timeout)
        + cushion
        - datetime.timedelta(seconds=1)
    )
    max_deadline = after + datetime.timedelta(seconds=timeout) + cushion
    assert min_deadline <= deadline <= max_deadline

    # assert: transfer items
    items = tdata["DATA"]
    assert len(items) == 1
    item = items[0]
    assert item["source_path"] == source
    assert item["destination_path"] == dest


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
    env = GlobusTransferEnv(
        GLOBUS_CLIENT_ID="cid",
        GLOBUS_CLIENT_SECRET="secret",
        GLOBUS_SOURCE_COLLECTION_ID="src",
        GLOBUS_DEST_COLLECTION_ID="dst",
    )
    mock_from_env.return_value = env

    token_resp = MagicMock()
    token_resp.by_resource_server = {
        "transfer.api.globus.org": {"access_token": "ACCESS-TOKEN"}
    }
    mock_confidential.return_value.oauth2_client_credentials_tokens.return_value = (
        token_resp
    )

    client = MagicMock()
    client.submit_transfer.return_value = {"task_id": "TASK-123"}
    mock_transfer_client.return_value = client

    # arrange: inputs
    tdata = MagicMock()

    # act
    gt = GlobusTransfer()
    tid = await gt._submit_transfer(tdata)

    # assert
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
    env = GlobusTransferEnv(
        GLOBUS_CLIENT_ID="cid",
        GLOBUS_CLIENT_SECRET="secret",
        GLOBUS_SOURCE_COLLECTION_ID="/src",
        GLOBUS_DEST_COLLECTION_ID="/dst",
    )
    mock_from_env.return_value = env

    token_resp = MagicMock()
    token_resp.by_resource_server = {
        "transfer.api.globus.org": {"access_token": "ACCESS-TOKEN"}
    }
    mock_confidential.return_value.oauth2_client_credentials_tokens.return_value = (
        token_resp
    )
    mock_transfer_client.return_value = MagicMock()

    # arrange: inputs
    source_path = "relative/path.dat"
    dest_url = "globus://dest/path"
    request_timeout = 10

    # act
    gt = GlobusTransfer()
    with pytest.raises(ValueError) as excinfo:
        await gt.transfer_file(
            source_path=source_path,
            dest_url=dest_url,
            request_timeout=request_timeout,
        )

    # assert
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
    env = GlobusTransferEnv(
        GLOBUS_CLIENT_ID="cid",
        GLOBUS_CLIENT_SECRET="secret",
        GLOBUS_SOURCE_COLLECTION_ID="src-id",
        GLOBUS_DEST_COLLECTION_ID="dst-id",
        GLOBUS_POLL_INTERVAL_SECONDS=1.0,
    )
    mock_from_env.return_value = env

    token_resp = MagicMock()
    token_resp.by_resource_server = {
        "transfer.api.globus.org": {"access_token": "ACCESS-TOKEN"}
    }
    mock_confidential.return_value.oauth2_client_credentials_tokens.return_value = (
        token_resp
    )

    client = MagicMock()
    client.submit_transfer.return_value = {"task_id": "TASK-123"}
    client.get_task.return_value = {"status": "SUCCEEDED"}
    mock_transfer_client.return_value = client

    # arrange: inputs
    source_path = "/abs/path.dat"
    dest_url = "globus://dest/path"
    request_timeout = 30

    # act
    gt = GlobusTransfer()
    result = await gt.transfer_file(
        source_path=source_path,
        dest_url=dest_url,
        request_timeout=request_timeout,
    )

    # assert: submit + single poll
    client.submit_transfer.assert_called_once()
    client.get_task.assert_called_once_with("TASK-123")
    assert result == "TASK-123"


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
    env = GlobusTransferEnv(
        GLOBUS_CLIENT_ID="cid",
        GLOBUS_CLIENT_SECRET="secret",
        GLOBUS_SOURCE_COLLECTION_ID="src-id",
        GLOBUS_DEST_COLLECTION_ID="dst-id",
        GLOBUS_POLL_INTERVAL_SECONDS=2.0,
    )
    mock_from_env.return_value = env

    token_resp = MagicMock()
    token_resp.by_resource_server = {
        "transfer.api.globus.org": {"access_token": "ACCESS-TOKEN"}
    }
    mock_confidential.return_value.oauth2_client_credentials_tokens.return_value = (
        token_resp
    )

    client = MagicMock()
    client.get_task.side_effect = [
        {"status": "ACTIVE"},
        {"status": "SUCCEEDED"},
    ]
    mock_transfer_client.return_value = client

    # arrange: inputs
    source_path = "/abs/file.dat"
    dest_url = "globus://dest/path"
    request_timeout = 30

    # arrange: instance overrides
    gt = GlobusTransfer()
    gt.make_transfer_document = MagicMock(return_value="TData")
    gt._submit_transfer = AsyncMock(return_value="TASK-123")

    # act
    result = await gt.transfer_file(
        source_path=source_path,
        dest_url=dest_url,
        request_timeout=request_timeout,
    )

    # assert
    assert client.get_task.call_count == 2
    mock_sleep.assert_awaited_once_with(2.0)
    assert result == "TASK-123"


@patch("lta.transfer.globus.IntervalTimer")
@patch("lta.transfer.globus.asyncio.sleep", new_callable=AsyncMock)
@patch("lta.transfer.globus.globus_sdk.AccessTokenAuthorizer")
@patch("lta.transfer.globus.globus_sdk.TransferClient")
@patch("lta.transfer.globus.globus_sdk.ConfidentialAppAuthClient")
@patch("lta.transfer.globus.from_environment_as_dataclass")
@pytest.mark.asyncio
async def test_430_transfer_file_timeout_cancels_and_raises(
    mock_from_env,
    mock_confidential,
    mock_transfer_client,
    mock_authorizer,
    mock_sleep,
    mock_timer,
) -> None:
    """When the deadline elapses, transfer_file cancels and raises TimeoutError."""
    # arrange: environment + SDK patches
    env = GlobusTransferEnv(
        GLOBUS_CLIENT_ID="cid",
        GLOBUS_CLIENT_SECRET="secret",
        GLOBUS_SOURCE_COLLECTION_ID="src-id",
        GLOBUS_DEST_COLLECTION_ID="dst-id",
        GLOBUS_POLL_INTERVAL_SECONDS=1.0,
    )
    mock_from_env.return_value = env

    token_resp = MagicMock()
    token_resp.by_resource_server = {
        "transfer.api.globus.org": {"access_token": "ACCESS-TOKEN"}
    }
    mock_confidential.return_value.oauth2_client_credentials_tokens.return_value = (
        token_resp
    )

    client = MagicMock()
    client.get_task.return_value = {"status": "ACTIVE"}
    mock_transfer_client.return_value = client

    mock_timer.return_value.has_interval_elapsed.return_value = True

    # arrange: inputs
    source_path = "/abs/file.dat"
    dest_url = "globus://dest/path"
    request_timeout = 5

    # arrange: instance overrides
    gt = GlobusTransfer()
    gt.make_transfer_document = MagicMock(return_value="TData")
    gt._submit_transfer = AsyncMock(return_value="TASK-123")
    gt._cancel_task = MagicMock()

    # act
    with pytest.raises(TimeoutError) as excinfo:
        await gt.transfer_file(
            source_path=source_path,
            dest_url=dest_url,
            request_timeout=request_timeout,
        )

    # assert
    assert "timed out" in str(excinfo.value)
    gt._cancel_task.assert_called_once()
    mock_sleep.assert_not_awaited()


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
    env = GlobusTransferEnv(
        GLOBUS_CLIENT_ID="cid",
        GLOBUS_CLIENT_SECRET="secret",
        GLOBUS_SOURCE_COLLECTION_ID="src-id",
        GLOBUS_DEST_COLLECTION_ID="dst-id",
    )
    mock_from_env.return_value = env

    token_resp = MagicMock()
    token_resp.by_resource_server = {
        "transfer.api.globus.org": {"access_token": "ACCESS-TOKEN"}
    }
    mock_confidential.return_value.oauth2_client_credentials_tokens.return_value = (
        token_resp
    )

    client = MagicMock()
    client.submit_transfer.return_value = {"task_id": "TASK-123"}
    client.get_task.return_value = {"status": "FAILED"}
    mock_transfer_client.return_value = client

    # arrange: inputs
    source_path = "/abs/file.dat"
    dest_url = "globus://dest/path"
    request_timeout = 30

    # act
    gt = GlobusTransfer()
    with pytest.raises(GlobusTransferFailedException) as excinfo:
        await gt.transfer_file(
            source_path=source_path,
            dest_url=dest_url,
            request_timeout=request_timeout,
        )

    # assert
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
    env = GlobusTransferEnv(
        GLOBUS_CLIENT_ID="cid",
        GLOBUS_CLIENT_SECRET="secret",
        GLOBUS_SOURCE_COLLECTION_ID="src-id",
        GLOBUS_DEST_COLLECTION_ID="dst-id",
    )
    mock_from_env.return_value = env

    token_resp = MagicMock()
    token_resp.by_resource_server = {
        "transfer.api.globus.org": {"access_token": "ACCESS-TOKEN"}
    }
    mock_confidential.return_value.oauth2_client_credentials_tokens.return_value = (
        token_resp
    )

    client = MagicMock()
    client.get_task.return_value = {"status": "INACTIVE"}
    mock_transfer_client.return_value = client

    # arrange: inputs
    source_path = "/abs/file.dat"
    dest_url = "globus://dest/path"
    request_timeout = 30

    # arrange: instance overrides
    gt = GlobusTransfer()
    gt.make_transfer_document = MagicMock(return_value="TData")
    gt._submit_transfer = AsyncMock(return_value="TASK-123")

    # act
    with pytest.raises(GlobusTransferFailedException) as excinfo:
        await gt.transfer_file(
            source_path=source_path,
            dest_url=dest_url,
            request_timeout=request_timeout,
        )

    # assert
    text = str(excinfo.value)
    assert "INACTIVE" in text
    assert "TASK-123" in text


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
    env = GlobusTransferEnv(
        GLOBUS_CLIENT_ID="cid",
        GLOBUS_CLIENT_SECRET="secret",
        GLOBUS_SOURCE_COLLECTION_ID="src-id",
        GLOBUS_DEST_COLLECTION_ID="dst-id",
        GLOBUS_POLL_INTERVAL_SECONDS=1.0,
    )
    mock_from_env.return_value = env

    token_resp = MagicMock()
    token_resp.by_resource_server = {
        "transfer.api.globus.org": {"access_token": "ACCESS-TOKEN"}
    }
    mock_confidential.return_value.oauth2_client_credentials_tokens.return_value = (
        token_resp
    )

    client = MagicMock()
    client.get_task.side_effect = [
        {"status": "FOO"},
        {"status": "SUCCEEDED"},
    ]
    mock_transfer_client.return_value = client

    # arrange: inputs
    source_path = "/abs/file.dat"
    dest_url = "globus://dest/path"
    request_timeout = 30

    # arrange: instance overrides
    gt = GlobusTransfer()
    gt.make_transfer_document = MagicMock(return_value="TData")
    gt._submit_transfer = AsyncMock(return_value="TASK-123")

    # act
    result = await gt.transfer_file(
        source_path=source_path,
        dest_url=dest_url,
        request_timeout=request_timeout,
    )

    # assert
    assert result == "TASK-123"
    assert client.get_task.call_count == 2
    mock_sleep.assert_awaited_once_with(1.0)
